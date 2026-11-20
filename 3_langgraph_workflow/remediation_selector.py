#!/usr/bin/env python3
"""Agent 3: remediation selector (and the executor that closes the outer loop).

The selector asks the Ansible MCP server for its approved commands
(tools/list is the allowlist) and assembles the remediation steps required
by the root cause. Steps that name a tool outside the approved list are
rejected before anything runs.

The executor runs the approved plan through the same Ansible MCP server,
then routes back to the anomaly detector with a post-remediation telemetry
window (the outer loop): the workflow only reports success once the service
is verified healthy again.

Verify:

    python3 3_langgraph_workflow/remediation_selector.py --self-check
"""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request

import llm
import mcp_client
import state

STEP = "agent 3 remediation selector"
SERVICE_UNIT = "aiops-demo.service"
RESTART_TOOL_CANDIDATES = ("restart_service", "run_module", "run_ansible_module", "run_playbook")
VERIFY_POLL_SECONDS = 2


def truncate(text, limit=300):
    text = (text or "").strip().replace("\n", " | ")
    return text if len(text) <= limit else text[: limit - 3] + "..."


def build_eda_payload(action, service_name, plan, root_cause):
    """The remediation event handed to Ansible through EDA."""
    return {
        "action": action,
        "service": service_name,
        "plan": [
            {"tool": step.get("tool", ""), "arguments": step.get("arguments", {})}
            for step in plan
        ],
        "root_cause": root_cause.get("summary", ""),
    }


def build_eda_request(payload, username, password):
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return urllib.request.Request(
        os.environ.get("EDA_EVENT_URL", "").strip(),
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {token}",
        },
        method="POST",
    )


def post_eda_event(payload):
    """Dispatch to the Basic-auth AAP event stream. Never publish its URL in findings."""
    url = os.environ.get("EDA_EVENT_URL", "").strip()
    if not url:
        return {"status": "not configured"}
    username = os.environ.get("EDA_EVENT_USERNAME", "aiops-demo")
    password = os.environ.get("EDA_EVENT_PASSWORD", "")
    if not url.startswith("https://") or not username or not password:
        return {"ok": False, "status": "requires HTTPS and event-stream credentials"}
    try:
        request = build_eda_request(payload, username, password)
        # A redirect is not an acknowledgement (and must not forward Basic auth).
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args):
                return None
        with urllib.request.build_opener(NoRedirect).open(request, timeout=15) as response:
            return {"ok": 200 <= response.status < 300, "status": f"accepted HTTP {response.status}"}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": f"rejected HTTP {exc.code}"}
    except (OSError, ValueError):
        return {"ok": False, "status": "event-stream connection failed"}


def _approved_plan(root_cause, tools, service_name):
    """Deterministic plan: restart the faulted unit with an approved tool."""
    names = [tool.get("name", "") for tool in tools]
    tool = next((name for name in RESTART_TOOL_CANDIDATES if name in names), None)
    if tool is None:
        return {"steps": [], "rationale": "no approved remediation tool available"}
    component = root_cause.get("component") or SERVICE_UNIT
    if tool == "restart_service":
        arguments = {"service": component}
    elif tool == "run_playbook":
        arguments = {"playbook": "restart-aiops-demo.yml", "extra_vars": {"service": component}}
    else:
        arguments = {
            "host": service_name,
            "module": "ansible.builtin.systemd",
            "arguments": {"name": component, "state": "restarted"},
        }
    return {
        "steps": [
            {
                "tool": tool,
                "arguments": arguments,
                "rationale": f"restart {component} on {service_name} to clear the error state",
            }
        ],
        "rationale": (
            f"Root cause points at {component}; restarting the unit is the approved "
            f"remediation and clears the fault state without touching other services."
        ),
    }


def _llm_plan(root_cause, tools, service_name):
    """Ask the LLM to assemble a plan strictly from the approved tools."""
    catalog = [
        {"name": tool.get("name"), "description": tool.get("description", ""),
         "inputSchema": tool.get("inputSchema", {})}
        for tool in tools
    ]
    prompt = (
        "You are a remediation selector agent in an autonomous ITOps workflow. "
        "Assemble the smallest safe remediation for the root cause below. You may "
        "ONLY use tools from the approved list returned by the Ansible MCP server; "
        "any other tool will be rejected.\n\n"
        "Reply with ONLY a JSON object:\n"
        '{"steps": [{"tool": "<approved tool name>", "arguments": {...}, '
        '"rationale": "..."}], "rationale": "..."}\n\n'
        f"Service: {service_name}\n"
        f"Root cause: {json.dumps(root_cause)[:800]}\n"
        f"Approved tools: {json.dumps(catalog)[:1500]}\n"
    )
    return llm.chat_json([{"role": "user", "content": prompt}])


def _approved_names(ansible_mcp):
    try:
        return [tool.get("name", "") for tool in ansible_mcp.list_tools()]
    except mcp_client.McpError:
        return []


def build_selector_node(ansible_mcp=None, use_llm=True, eda=False):
    """Return the LangGraph node that assembles the approved remediation plan."""

    def selector_node(workflow_state):
        from langgraph.types import Command

        service_name = workflow_state["service_name"]
        root_cause = workflow_state.get("root_cause", {})
        if eda:
            # This stream's rulebook contract is a restart of the demo unit.
            # It is not an arbitrary Ansible module or shell execution endpoint.
            if (service_name not in os.environ.get("DASHBOARD_VMS", "").split()
                    or root_cause.get("component") != SERVICE_UNIT
                    or root_cause.get("confidence") != "high"):
                return Command(goto="__end__", update={"status": state.STATUS_FAILED,
                    "findings": [f"[{STEP}] no matching approved EDA restart action"]})
            return Command(goto=state.NODE_EXECUTOR, update={
                "status": state.STATUS_REMEDIATING,
                "remediation": {"approved_tools": ["restart_service"], "plan": [
                    {"tool": "restart_service", "arguments": {"service": SERVICE_UNIT}}]},
                "findings": [f"[{STEP}] request EDA restart of {SERVICE_UNIT} on {service_name}"]})
        approved = _approved_names(ansible_mcp) if ansible_mcp is not None else []
        tools = [{"name": name} for name in approved]

        plan = None
        if use_llm and llm.available() and approved:
            try:
                plan = _llm_plan(root_cause, tools, service_name)
            except Exception:  # noqa: BLE001 - LLM is best effort
                plan = None
        if plan is None:
            plan = _approved_plan(root_cause, tools, service_name) if tools else {
                "steps": [], "rationale": "Ansible MCP server not configured"
            }

        steps = []
        rejected = []
        for step in plan.get("steps", []):
            if isinstance(step, dict) and step.get("tool") in approved:
                steps.append(step)
            else:
                rejected.append(step.get("tool") if isinstance(step, dict) else str(step))
        remediation = {
            "plan": steps,
            "rationale": plan.get("rationale", ""),
            "approved_tools": approved,
        }
        findings = [
            f"[{STEP}] approved tools: {', '.join(approved) or 'none'}",
            f"[{STEP}] plan ({len(steps)} step(s)): " + (
                "; ".join(
                    f"{index + 1}) {step['tool']} {json.dumps(step['arguments'])} - {step.get('rationale', '')}"
                    for index, step in enumerate(steps)
                )
                or "no approved remediation possible"
            ),
        ]
        for name in rejected:
            findings.append(f"[{STEP}] rejected unapproved step: {name}")
        if not steps:
            update = {
                "remediation": remediation,
                "status": state.STATUS_FAILED,
                "findings": findings + [f"[{STEP}] no approved remediation available; giving up"],
            }
            return Command(goto="__end__", update=update)
        update = {
            "remediation": remediation,
            "status": state.STATUS_REMEDIATING,
            "findings": findings,
        }
        return Command(goto=state.NODE_EXECUTOR, update=update)

    return selector_node


def build_executor_node(ansible_mcp=None, get_window=None, verify_wait_seconds=30, deliver=None):
    """Return the LangGraph node that executes the plan and re-verifies."""

    def executor_node(workflow_state):
        from langgraph.types import Command

        service_name = workflow_state["service_name"]
        window_seconds = workflow_state.get("window_seconds", state.DEFAULT_WINDOW_SECONDS)
        plan = workflow_state.get("remediation", {}).get("plan", [])
        approved = set(workflow_state.get("remediation", {}).get("approved_tools", []))
        attempts = workflow_state.get("attempts", 0) + 1
        max_attempts = workflow_state.get("max_attempts", state.DEFAULT_MAX_ATTEMPTS)

        results = []
        ok = True
        if deliver is not None:
            eda_record = deliver(build_eda_payload("restart", service_name, plan,
                                                   workflow_state.get("root_cause", {})))
            ok = eda_record.get("ok", False)
            results.append({"step": plan[0], "ok": ok, "output": eda_record["status"]})
        else:
            eda_record = {"status": "not configured"}
        for step in ([] if deliver is not None else plan):
            tool = step.get("tool", "")
            if tool not in approved:
                results.append({"step": step, "ok": False, "output": "unapproved tool"})
                ok = False
                continue
            try:
                output = ansible_mcp.call_tool(tool, step.get("arguments") or {})
                results.append({"step": step, "ok": True, "output": truncate(output)})
            except (mcp_client.McpError, OSError, AttributeError) as exc:
                results.append({"step": step, "ok": False, "output": truncate(str(exc))})
                ok = False
        findings = [
            f"[{STEP} executor] attempt {attempts}/{max_attempts}: "
            + ("; ".join(
                f"{index + 1}) {'ok' if item['ok'] else 'FAILED'} {item['step']['tool']} -> {item['output'][:120]}"
                for index, item in enumerate(results)
            ) or "nothing to run")
        ]
        update = {
            "attempts": attempts,
            "remediation_result": {"steps": results, "ok": ok, "attempts": attempts, "eda": eda_record},
            "findings": findings,
        }
        if not ok:
            update["status"] = state.STATUS_FAILED
            return Command(goto="__end__", update=update)

        findings.append(f"[{STEP} executor] EDA delivery: {eda_record['status']}")

        # Outer loop: verify against telemetry received after the remediation.
        verify_since = time.monotonic()
        telemetry = workflow_state.get("telemetry", {})
        if get_window is not None:
            telemetry = get_window(service_name, window_seconds, since=verify_since)
            deadline = time.monotonic() + max(0, verify_wait_seconds)
            while time.monotonic() < deadline:
                if telemetry.get("metrics"):
                    break  # fresh post-remediation telemetry is arriving
                time.sleep(VERIFY_POLL_SECONDS)
                telemetry = get_window(service_name, window_seconds, since=verify_since)
        findings.append(f"[{STEP} executor] handing over to the anomaly detector for verification")
        update.update(
            {
                "telemetry": telemetry,
                "verify_since": verify_since,
                "iteration": 0,
                "status": state.STATUS_REMEDIATING,
                "findings": findings,
            }
        )
        return Command(goto=state.NODE_DETECTOR, update=update)

    return executor_node


def self_check():
    from langgraph.types import Command

    import mock_mcp_server

    ansible = mcp_client.FakeMcpClient(mock_mcp_server.ansible_tools(), mock_mcp_server.ansible_call)
    selector = build_selector_node(ansible_mcp=ansible, use_llm=False)
    root_cause = {
        "found": True,
        "component": SERVICE_UNIT,
        "summary": "dummy-crash fault state persists until restart",
        "confidence": "high",
    }
    workflow_state = state.initial_state("vm-1")
    workflow_state["root_cause"] = root_cause
    result = selector(workflow_state)
    if result.goto != state.NODE_EXECUTOR:
        raise SystemExit(f"{STEP}: selector should route to executor, got {result.goto}")
    plan = result.update["remediation"]["plan"]
    if len(plan) != 1 or plan[0]["tool"] != "run_module":
        raise SystemExit(f"{STEP}: unexpected plan {plan}")
    if plan[0]["arguments"]["module"] != "ansible.builtin.systemd":
        raise SystemExit(f"{STEP}: plan should use systemd module {plan[0]['arguments']}")

    # Unapproved tools must be rejected before execution.
    class BadLlm:
        @staticmethod
        def chat_json(messages):
            return {"steps": [{"tool": "format_disk", "arguments": {}}], "rationale": "nope"}

    saved = llm.chat_json
    llm.chat_json = BadLlm.chat_json
    try:
        saved_available = llm.available
        llm.available = lambda: True
        strict_selector = build_selector_node(ansible_mcp=ansible, use_llm=True)
        result = strict_selector(workflow_state)
    finally:
        llm.chat_json = saved
        llm.available = saved_available
    if result.goto != "__end__" or result.update["status"] != state.STATUS_FAILED:
        raise SystemExit(f"{STEP}: unapproved plan must fail safely, got {result.goto}")
    if result.update["remediation"]["plan"]:
        raise SystemExit(f"{STEP}: unapproved step must not survive {result.update['remediation']}")

    # Executor: run the plan, then hand back to the detector with fresh telemetry.
    healthy = {
        "ever_seen": True,
        "last_seen_seconds_ago": 1.0,
        "metrics": {"service.uptime": 1.0},
        "logs": [],
        "traces": [],
    }
    windows = {"calls": 0}

    def get_window(service, seconds, since=None):
        windows["calls"] += 1
        return healthy

    executor = build_executor_node(ansible_mcp=ansible, get_window=get_window, verify_wait_seconds=0)
    workflow_state = state.initial_state("vm-1")
    workflow_state["root_cause"] = root_cause
    selector_result = selector(workflow_state)
    for key, value in selector_result.update.items():
        if key in ("findings",):
            workflow_state[key] = workflow_state.get(key, []) + value
        else:
            workflow_state[key] = value
    result = executor(workflow_state)
    if result.goto != state.NODE_DETECTOR:
        raise SystemExit(f"{STEP}: executor should verify via detector, got {result.goto}")
    if result.update["attempts"] != 1 or not result.update["remediation_result"]["ok"]:
        raise SystemExit(f"{STEP}: executor result wrong {result.update['remediation_result']}")
    if result.update["telemetry"] is not healthy:
        raise SystemExit(f"{STEP}: executor should refresh telemetry for verification")

    # A failing tool must end the workflow as failed, not loop.
    def fail_handler(name, arguments):
        raise ValueError("ansible unreachable")

    failing = mcp_client.FakeMcpClient(mock_mcp_server.ansible_tools(), fail_handler)
    executor = build_executor_node(ansible_mcp=failing, get_window=get_window, verify_wait_seconds=0)
    result = executor(workflow_state)
    if result.goto != "__end__" or result.update["status"] != state.STATUS_FAILED:
        raise SystemExit(f"{STEP}: failed execution should stop, got {result.goto}")

    # EDA dispatch is exclusive: no MCP client and no real POST in self-checks.
    from unittest.mock import patch
    events = []
    with patch.dict(os.environ, {"DASHBOARD_VMS": "vm-1"}):
        select = build_selector_node(eda=True)
        selected = select(workflow_state)
        assert selected.goto == state.NODE_EXECUTOR
        assert select({**workflow_state, "service_name": "other"}).goto == "__end__"
    workflow_state.update(selected.update)
    def accept(payload):
        events.append(payload)
        return {"ok": True, "status": "accepted HTTP 202"}
    dispatched = build_executor_node(get_window=get_window, verify_wait_seconds=0,
                                     deliver=accept)(workflow_state)
    assert dispatched.goto == state.NODE_DETECTOR
    assert len(events) == 1 and events[0]["service"] == "vm-1" and events[0]["action"] == "restart"
    rejected = build_executor_node(deliver=lambda _: {"ok": False, "status": "rejected HTTP 401"})(workflow_state)
    assert rejected.goto == "__end__" and rejected.update["status"] == state.STATUS_FAILED

    # EDA delivery: off without EDA_EVENT_URL; payload and request buildable.
    saved = {key: os.environ.get(key) for key in
             ("EDA_EVENT_URL", "EDA_EVENT_USERNAME", "EDA_EVENT_PASSWORD")}
    os.environ.pop("EDA_EVENT_URL", None)
    try:
        record = post_eda_event(build_eda_payload("restart", "vm-1", [], {}))
        if record["status"] != "not configured":
            raise SystemExit(f"{STEP}: EDA should be off without EDA_EVENT_URL: {record}")
        payload = build_eda_payload(
            "restart", "vm-1",
            [{"tool": "run_module", "arguments": {"module": "ansible.builtin.systemd"}}],
            {"summary": "unit down"},
        )
        if (payload["action"] != "restart" or payload["plan"][0]["tool"] != "run_module"
                or payload["root_cause"] != "unit down"):
            raise SystemExit(f"{STEP}: EDA payload wrong: {payload}")
        os.environ["EDA_EVENT_URL"] = "https://eda.example.com/stream"
        request = build_eda_request(payload, "u", "p")
        if request.full_url != "https://eda.example.com/stream":
            raise SystemExit(f"{STEP}: EDA request url wrong: {request.full_url}")
        if request.headers.get("Authorization") != "Basic dTpw":
            raise SystemExit(f"{STEP}: EDA auth header wrong")
        if json.loads(request.data) != payload:
            raise SystemExit(f"{STEP}: EDA request body wrong")
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    print(f"{STEP}: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Agent 3: remediation selector")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not args.self_check:
        raise SystemExit("pass --self-check; the workflow runs this agent as a graph node")
    self_check()


if __name__ == "__main__":
    main()
