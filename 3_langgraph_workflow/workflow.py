#!/usr/bin/env python3
"""Run the LangGraph multi-agent ITOps workflow for autonomous remediation.

Pipeline (one graph run per anomalous service):

  1. collector.py receives OTLP/HTTP metrics, logs and traces (:4318).
  2. agent 1 anomaly_detector examines a time window of that telemetry.
  3. agent 2 root_cause_analyzer iterates through MCP tools (inner loop).
  4. agent 3 remediation_selector assembles approved steps from the
     Ansible MCP server; the executor runs them and the anomaly detector
     verifies the result (outer loop).

Modes:

    python3 3_langgraph_workflow/workflow.py --self-check
        verify every module offline (no cluster, no LLM, no MCP servers)

    python3 3_langgraph_workflow/workflow.py --demo
        offline end-to-end run with mock MCP servers and synthetic telemetry

    python3 3_langgraph_workflow/workflow.py --run-once [--service NAME]
        collect live telemetry, wait for an anomaly, run the workflow once

    python3 3_langgraph_workflow/workflow.py
        collector + poller + dashboard on :4318 (the deployed mode)

Environment:

    LLM_BASE_URL, LLM_API_KEY, LLM_MODEL        OpenAI-compatible endpoint
    LINUX_MCP_URL / LINUX_MCP_COMMAND           RCA MCP server (linux-mcp)
    ANSIBLE_MCP_URL / ANSIBLE_MCP_COMMAND       remediation MCP server
    MCP_INSECURE_TLS=1                          skip TLS verification
"""

import argparse
import html
import json
import os
import threading
import time
from http.server import ThreadingHTTPServer

import anomaly_detector
import collector
import graph
import llm
import mcp_client
import mock_mcp_server
import remediation_selector
import root_cause_analyzer
import state

STEP = "aiops workflow"


def build_deps(args, store=None):
    """Build the graph with real clients from the environment (or overrides)."""
    insecure = getattr(args, "insecure_tls", False)

    def client_for(prefix, url_flag, command_flag):
        url = (url_flag or "").strip() or None
        command = (command_flag or "").strip() or None
        if url or command:
            return mcp_client.from_config(url=url, command=command, insecure=insecure)
        return mcp_client.from_env(prefix, insecure=insecure)

    linux = client_for("LINUX", getattr(args, "linux_mcp_url", None), getattr(args, "linux_mcp_command", None))
    ansible = client_for("ANSIBLE", getattr(args, "ansible_mcp_url", None), getattr(args, "ansible_mcp_command", None))

    get_window = None
    if store is not None:
        def get_window(service, seconds, since=None):
            return store.window(service, seconds, since=since)

    app = graph.build_graph(
        linux_mcp=linux,
        ansible_mcp=ansible,
        get_window=get_window,
        use_llm=True,
        silence_seconds=args.silence_seconds,
        verify_wait_seconds=args.verify_wait_seconds,
    )
    return app, linux, ansible


def close_clients(*clients):
    for client in clients:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001 - shutdown is best effort
                pass


def run_offline_demo():
    """Full pipeline with in-process mock MCP servers and synthetic telemetry."""
    print("OFFLINE DEMO: mock MCP servers, synthetic telemetry, no LLM required", flush=True)
    mock_mcp_server.reset()
    linux = mcp_client.FakeMcpClient(mock_mcp_server.linux_tools(), mock_mcp_server.linux_call)
    ansible = mcp_client.FakeMcpClient(mock_mcp_server.ansible_tools(), mock_mcp_server.ansible_call)
    anomaly_flag = {"anomalous": True}

    def get_window(service, seconds, since=None):
        if mock_mcp_server.service_restarted():
            anomaly_flag["anomalous"] = False
        if anomaly_flag["anomalous"]:
            return {
                "ever_seen": True,
                "last_seen_seconds_ago": 3.0,
                "metrics": {"service.uptime": 187.4, "service.cpu.load": 6.2,
                            "service.memory.load": 31.0, "dummy-crash": 1},
                "metric_events": [
                    {"metric": "dummy-crash", "from": 0, "to": 1, "age_seconds": 3.0}
                ],
                "logs": [{"severity_number": 17, "severity_text": "ERROR",
                          "body": "service app simulated a dummy crash"}],
                "traces": [{"name": "dummy-crash", "status_code": 2,
                            "status_message": "dummy crash"}],
            }
        return {
            "ever_seen": True,
            "last_seen_seconds_ago": 1.0,
            "metrics": {"service.uptime": 2.0, "service.cpu.load": 4.0},
            "metric_events": [],
            "logs": [],
            "traces": [],
        }

    app = graph.build_graph(
        linux_mcp=linux,
        ansible_mcp=ansible,
        get_window=get_window,
        use_llm=False,
        verify_wait_seconds=0,
    )
    service_name = "rhel10-service-1"
    print(f"{STEP}: anomaly injected into the telemetry window for {service_name}", flush=True)
    final = graph.run(app, state.initial_state(
        service_name,
        telemetry=get_window(service_name, 120),
        max_iterations=5,
        max_attempts=3,
    ), label=service_name)
    print(f"{STEP}: final status: {final.get('status')}", flush=True)
    root_cause = final.get("root_cause", {})
    print(f"{STEP}: root cause: {root_cause.get('summary', 'n/a')}", flush=True)
    print(f"{STEP}: attempts: {final.get('attempts')}  "
          f"investigation steps: {len(final.get('investigation', []))}  "
          f"findings: {len(final.get('findings', []))}", flush=True)
    return final


class WorkflowHandler(collector.CollectorHandler):
    """Collector endpoints plus the multi-agent workflow dashboard."""

    runs = {}  # service_name -> latest final state (or {"running": True})
    runs_lock = threading.Lock()

    def do_GET(self):
        from urllib.parse import parse_qs, urlparse

        parsed_url = urlparse(self.path)
        if parsed_url.path == "/runs":
            with self.runs_lock:
                payload = dict(self.runs)
            body = json.dumps(payload, indent=2, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed_url.path in ("/", "/dashboard"):
            page = render_workflow_dashboard(self.store, self.runs, self.runs_lock).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        super().do_GET()


def _fmt_duration(seconds):
    seconds = max(0, int(seconds))
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _pill(text, cls):
    return f'<span class="pill {cls}">{html.escape(str(text))}</span>'


def render_workflow_dashboard(store, runs, runs_lock):
    """Unified dashboard, built only from telemetry and workflow run state:

    (1) VM status (telemetry view), (2) service status per VM, (3) anomalies
    detected, (4) RCA output, (5) the recommended remediation destined for
    Ansible through EDA."""
    with runs_lock:
        runs_snapshot = dict(runs)

    window_seconds = 600
    names = sorted(set(store.services()) | set(runs_snapshot))

    vm_rows, service_rows = [], []
    for name in names:
        window = store.window(name, window_seconds)
        metrics = window.get("metrics", {})
        logs = window.get("logs", [])
        traces = window.get("traces", [])
        events = window.get("metric_events", [])
        last_seen = window.get("last_seen_seconds_ago")
        if not window.get("ever_seen"):
            vm_status = _pill("no telemetry yet", "muted")
        elif last_seen is not None and last_seen <= state.DEFAULT_SILENCE_SECONDS:
            vm_status = _pill("telemetry flowing", "ok")
        else:
            vm_status = _pill(f"silent {last_seen:.0f}s", "bad")
        observed = window.get("observed_for_seconds")
        vm_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{vm_status}</td>"
            f"<td>{'-' if last_seen is None else f'{last_seen:.0f}s ago'}</td>"
            f"<td>{'-' if not observed else _fmt_duration(observed)}</td>"
            f"<td>{len(metrics)}</td>"
            f"<td>{len(logs)} ({sum(1 for item in logs if item.get('severity_number', 0) >= 17)} err)</td>"
            f"<td>{len(traces)} ({sum(1 for item in traces if item.get('status_code') == 2)} failed)</td>"
            "</tr>"
        )
        uptime = metrics.get("service.uptime")
        restarts = sum(1 for item in events if item.get("metric") == "service.uptime")
        crash = metrics.get("dummy-crash", 0)
        fault = _pill(f"fault flag {crash}", "bad") if crash else _pill("clear", "ok")
        cpu = metrics.get("service.cpu.load")
        memory = metrics.get("service.memory.load")
        service_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{'-' if uptime is None else _fmt_duration(uptime)}</td>"
            f"<td>{restarts}</td>"
            f"<td>{'-' if cpu is None else f'{cpu:.1f}%'}</td>"
            f"<td>{'-' if memory is None else f'{memory:.1f}%'}</td>"
            f"<td>{fault}</td>"
            "</tr>"
        )

    anomaly_rows, rca_rows, remediation_rows = [], [], []
    for name in names:
        run = runs_snapshot.get(name)
        if not run or run.get("running"):
            note = "workflow running" if run else "no run yet"
            cell = _pill(note, "muted")
            anomaly_rows.append(f"<tr><td>{html.escape(name)}</td><td>{cell}</td>"
                                "<td colspan=\"4\"></td></tr>")
            rca_rows.append(f"<tr><td>{html.escape(name)}</td><td>{cell}</td>"
                            "<td></td><td></td><td></td></tr>")
            remediation_rows.append(f"<tr><td>{html.escape(name)}</td><td>{cell}</td>"
                                    "<td></td><td></td><td></td></tr>")
            continue
        anomaly = run.get("anomaly", {})
        detected = anomaly.get("detected")
        status = _pill("anomaly", "bad") if detected else _pill("healthy", "ok")
        anomaly_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{status}</td>"
            f"<td>{html.escape(', '.join(anomaly.get('signals', [])) or '-')}</td>"
            f"<td>{html.escape(str(anomaly.get('severity', '-')))}</td>"
            f"<td>{html.escape(str(anomaly.get('summary', ''))[:220])}</td>"
            f"<td>{run.get('attempts', 0)}/{run.get('max_attempts', state.DEFAULT_MAX_ATTEMPTS)}</td>"
            "</tr>"
        )
        root_cause = run.get("root_cause", {})
        confidence = root_cause.get("confidence", "n/a")
        cls = "ok" if confidence == "high" else ("warn" if confidence == "medium" else "muted")
        rca_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{html.escape(str(root_cause.get('component', '-')))}</td>"
            f"<td>{_pill(confidence, cls)}</td>"
            f"<td>{html.escape(str(root_cause.get('summary', ''))[:300])}</td>"
            f"<td>{len(run.get('investigation', []))}</td>"
            "</tr>"
        )
        remediation = run.get("remediation", {})
        plan = remediation.get("plan", [])
        plan_html = "<br>".join(
            f"{index + 1}. {html.escape(step.get('tool', ''))} "
            f"{html.escape(json.dumps(step.get('arguments', {})))[:120]}"
            for index, step in enumerate(plan)
        ) or "none"
        result = run.get("remediation_result", {})
        if result.get("ok"):
            result_pill = _pill("executed ok", "ok")
        elif result:
            result_pill = _pill("failed", "bad")
        else:
            result_pill = _pill("pending", "muted")
        eda = (result.get("eda") or {}).get("status", "not configured")
        remediation_rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{plan_html}</td>"
            f"<td>{result.get('attempts', run.get('attempts', 0))}</td>"
            f"<td>{result_pill}</td>"
            f"<td>{html.escape(str(eda))}</td>"
            "</tr>"
        )

    findings_rows = []
    for name in sorted(runs_snapshot):
        run = runs_snapshot[name]
        if not run or run.get("running"):
            continue
        for line in run.get("findings", [])[-12:]:
            findings_rows.append(
                f"<tr><td>{html.escape(name)}</td><td>{html.escape(line)}</td></tr>"
            )
    findings_body = ("\n".join(findings_rows)) or "<tr><td colspan=\"2\">No agent findings yet.</td></tr>"
    llm_state = "on" if llm.available() else "off (heuristic fallback)"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>AIOps unified view</title>
  <style>
    body {{ font-family: "Segoe UI", Helvetica, Arial, sans-serif; margin: 2rem; background: #f4f6f8; color: #1f2933; }}
    main {{ max-width: 76rem; }}
    h2 {{ margin: 2rem 0 0.5rem; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border: 1px solid #e2e8f0; padding: 0.5rem 0.8rem; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f6; }}
    .pill {{ padding: 0.05rem 0.55rem; border-radius: 999px; font-size: 0.85rem; white-space: nowrap; }}
    .ok {{ background: #dff5e7; color: #0f7b3a; }}
    .bad {{ background: #fde5e2; color: #b42318; }}
    .warn {{ background: #fdf0d7; color: #a15c00; }}
    .muted {{ background: #eef2f6; color: #52606d; }}
    p.meta {{ color: #52606d; }}
  </style>
</head>
<body>
  <main>
    <h1>AIOps unified view &mdash; autonomous remediation</h1>
    <p class="meta">{len(names)} services &middot; built from telemetry only &middot; llm: {llm_state}</p>

    <h2>1. Status of VMs (telemetry view)</h2>
    <table>
      <thead><tr><th>VM</th><th>Telemetry</th><th>Last export</th><th>Observed for</th><th>Metrics</th><th>Logs</th><th>Traces</th></tr></thead>
      <tbody>{''.join(vm_rows) or no_telemetry_row(7)}</tbody>
    </table>

    <h2>2. Service status per VM (telemetry view)</h2>
    <table>
      <thead><tr><th>VM</th><th>Process uptime</th><th>Restarts (10m)</th><th>Host CPU</th><th>Host memory</th><th>Fault state</th></tr></thead>
      <tbody>{''.join(service_rows) or no_telemetry_row(6)}</tbody>
    </table>

    <h2>3. Anomalies detected</h2>
    <table>
      <thead><tr><th>VM</th><th>Status</th><th>Signals</th><th>Severity</th><th>Summary</th><th>Attempts</th></tr></thead>
      <tbody>{''.join(anomaly_rows)}</tbody>
    </table>

    <h2>4. RCA output</h2>
    <table>
      <thead><tr><th>VM</th><th>Component</th><th>Confidence</th><th>Root cause</th><th>Steps</th></tr></thead>
      <tbody>{''.join(rca_rows)}</tbody>
    </table>

    <h2>5. Recommended remediation &rarr; Ansible (EDA)</h2>
    <table>
      <thead><tr><th>VM</th><th>Plan</th><th>Attempts</th><th>Execution</th><th>EDA delivery</th></tr></thead>
      <tbody>{''.join(remediation_rows)}</tbody>
    </table>

    <h2>Agent findings (latest run, newest last)</h2>
    <table>
      <thead><tr><th>VM</th><th>Finding</th></tr></thead>
      <tbody>{findings_body}</tbody>
    </table>
  </main>
</body>
</html>
"""


def no_telemetry_row(columns):
    return f"<tr><td colspan=\"{columns}\">No telemetry collected yet.</td></tr>"


def serve_forever(args):
    """Deployed mode: collector + poller + dashboard on one port."""
    store = collector.TelemetryStore()
    WorkflowHandler.store = store
    server = ThreadingHTTPServer((args.host, args.port), WorkflowHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="http").start()
    print(
        f"{STEP}: collector listening on {args.host}:{args.port} "
        f"(OTLP metrics/logs/traces + dashboard at /)",
        flush=True,
    )

    running = set()
    finished_at = {}

    def run_for_service(service_name):
        with WorkflowHandler.runs_lock:
            WorkflowHandler.runs[service_name] = {"running": True}
        try:
            app, linux, ansible = build_deps(args, store=store)
            telemetry = store.window(service_name, args.window_seconds)
            workflow_state = state.initial_state(
                service_name,
                window_seconds=args.window_seconds,
                telemetry=telemetry,
                max_iterations=args.max_iterations,
                max_attempts=args.max_attempts,
            )
            final = graph.run(app, workflow_state, label=service_name)
            with WorkflowHandler.runs_lock:
                WorkflowHandler.runs[service_name] = final or {"status": "unknown"}
            close_clients(linux, ansible)
        except Exception as exc:  # noqa: BLE001 - a failed run must not kill the poller
            print(f"{STEP}: run for {service_name} failed: {exc}", flush=True)
            with WorkflowHandler.runs_lock:
                WorkflowHandler.runs[service_name] = {"running": False, "status": "error", "error": str(exc)}
        finally:
            running.discard(service_name)
            finished_at[service_name] = time.monotonic()

    print(f"{STEP}: poller started (window={args.window_seconds}s silence={args.silence_seconds}s "
          f"cooldown={args.cooldown_seconds}s)", flush=True)
    try:
        while True:
            time.sleep(args.poll_seconds)
            for service_name in store.services():
                if service_name in running:
                    continue
                if time.monotonic() - finished_at.get(service_name, 0) < args.cooldown_seconds:
                    continue
                window = store.window(service_name, args.window_seconds)
                quick = anomaly_detector.analyze_window(window, args.silence_seconds)
                if quick["detected"]:
                    print(f"{STEP}: anomaly detected for {service_name} "
                          f"({','.join(quick['signals'])}); starting workflow run", flush=True)
                    running.add(service_name)
                    threading.Thread(
                        target=run_for_service, args=(service_name,), daemon=True,
                        name=f"run-{service_name}",
                    ).start()
    except KeyboardInterrupt:
        print(f"{STEP}: stopped", flush=True)
    finally:
        server.shutdown()
        server.server_close()


def run_once(args):
    """Collect live telemetry, wait for an anomaly, run the workflow once."""
    store = collector.TelemetryStore()
    WorkflowHandler.store = store
    server = ThreadingHTTPServer((args.host, args.port), WorkflowHandler)
    threading.Thread(target=server.serve_forever, daemon=True, name="http").start()
    print(f"{STEP}: collecting telemetry on {args.host}:{args.port}; waiting for an anomaly "
          f"(timeout {args.wait_seconds}s)", flush=True)
    try:
        deadline = time.monotonic() + args.wait_seconds
        target = None
        while time.monotonic() < deadline:
            time.sleep(args.poll_seconds)
            names = [args.service] if args.service else store.services()
            for service_name in names:
                if not store.window(service_name, 1)["ever_seen"]:
                    continue
                window = store.window(service_name, args.window_seconds)
                quick = anomaly_detector.analyze_window(window, args.silence_seconds)
                if quick["detected"]:
                    target = service_name
                    print(f"{STEP}: anomaly detected for {service_name}: {quick['summary']}", flush=True)
                    break
            if target:
                break
        if target is None:
            print(f"{STEP}: no anomaly within {args.wait_seconds}s", flush=True)
            return None
        # The collector keeps running: the executor verifies the remediation
        # against telemetry that arrives after it, so the service must still
        # be able to push while the graph executes.
        app, linux, ansible = build_deps(args, store=store)
        try:
            telemetry = store.window(target, args.window_seconds)
            final = graph.run(app, state.initial_state(
                target,
                window_seconds=args.window_seconds,
                telemetry=telemetry,
                max_iterations=args.max_iterations,
                max_attempts=args.max_attempts,
            ), label=target)
            print(f"{STEP}: final status: {(final or {}).get('status')}", flush=True)
            return final
        finally:
            close_clients(linux, ansible)
    finally:
        server.shutdown()
        server.server_close()


def self_check():
    for module in (state, llm, mock_mcp_server, mcp_client, collector, anomaly_detector,
                   root_cause_analyzer, remediation_selector, graph):
        module.self_check()

    # The unified dashboard renders all five sections from telemetry + runs.
    store = collector.TelemetryStore()
    now = time.monotonic()
    store.record_metrics(collector.parse_metrics(
        collector.sample_metrics_payload(
            "vm-1", {"service.uptime": 42.0, "service.cpu.load": 3.0,
                     "service.memory.load": 12.0, "dummy-crash": 1}),
        received_at=now - 2))
    run = state.initial_state("vm-1", telemetry={})
    run.update({
        "status": state.STATUS_REMEDIATED,
        "anomaly": {"detected": True, "signals": ["metrics-silent"], "severity": "high",
                    "summary": "no telemetry for 42s"},
        "root_cause": {"found": True, "component": "aiops-demo.service",
                       "confidence": "high", "summary": "unit is down"},
        "remediation": {"plan": [{"tool": "run_module",
                                  "arguments": {"module": "ansible.builtin.systemd"},
                                  "rationale": "restart"}], "rationale": ""},
        "remediation_result": {"ok": True, "attempts": 1, "steps": [],
                               "eda": {"status": "not configured"}},
        "investigation": [{"iteration": 1, "tool": "get_service_status"}],
        "findings": ["[agent 1 anomaly detector] example"],
    })
    page = render_workflow_dashboard(store, {"vm-1": run}, threading.Lock())
    for expected in ("vm-1", "Status of VMs", "Service status per VM", "Anomalies detected",
                     "RCA output", "Recommended remediation", "Ansible (EDA)",
                     "telemetry flowing", "aiops-demo.service", "run_module",
                     "not configured", "fault flag 1"):
        if expected not in page:
            raise SystemExit(f"{STEP}: dashboard missing {expected!r}")

    final = run_offline_demo()
    if final.get("status") != state.STATUS_REMEDIATED:
        raise SystemExit(f"{STEP}: offline demo should end REMEDIATED, got {final.get('status')}")
    print(f"{STEP}: self-check ok", flush=True)


def _env_int(name, fallback):
    try:
        return int(os.environ.get(name, "") or fallback)
    except ValueError:
        return fallback


def _env_float(name, fallback):
    try:
        return float(os.environ.get(name, "") or fallback)
    except ValueError:
        return fallback


def main():
    parser = argparse.ArgumentParser(description="Multi-agent AIOps workflow (LangGraph)")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--demo", action="store_true", help="offline end-to-end demo with mock MCP servers")
    parser.add_argument("--run-once", action="store_true", help="wait for one anomaly, run once, exit")
    parser.add_argument("--service", default="", help="limit to one service name")
    parser.add_argument("--host", default=collector.LISTEN_HOST)
    parser.add_argument("--port", type=int, default=collector.LISTEN_PORT)
    parser.add_argument("--window-seconds", type=int,
                        default=_env_int("WINDOW_SECONDS", state.DEFAULT_WINDOW_SECONDS))
    parser.add_argument("--silence-seconds", type=float,
                        default=_env_float("SILENCE_SECONDS", state.DEFAULT_SILENCE_SECONDS))
    parser.add_argument("--poll-seconds", type=float,
                        default=_env_float("POLL_SECONDS", state.DEFAULT_POLL_SECONDS))
    parser.add_argument("--cooldown-seconds", type=float,
                        default=_env_float("COOLDOWN_SECONDS", state.DEFAULT_COOLDOWN_SECONDS))
    parser.add_argument("--verify-wait-seconds", type=float,
                        default=_env_float("VERIFY_WAIT_SECONDS", state.DEFAULT_VERIFY_WAIT_SECONDS))
    parser.add_argument("--wait-seconds", type=float, default=300, help="--run-once anomaly timeout")
    parser.add_argument("--max-iterations", type=int,
                        default=_env_int("MAX_ITERATIONS", state.DEFAULT_MAX_ITERATIONS))
    parser.add_argument("--max-attempts", type=int,
                        default=_env_int("MAX_ATTEMPTS", state.DEFAULT_MAX_ATTEMPTS))
    parser.add_argument("--linux-mcp-url", default="", help="override LINUX_MCP_URL")
    parser.add_argument("--linux-mcp-command", default="", help="override LINUX_MCP_COMMAND")
    parser.add_argument("--ansible-mcp-url", default="", help="override ANSIBLE_MCP_URL")
    parser.add_argument("--ansible-mcp-command", default="", help="override ANSIBLE_MCP_COMMAND")
    parser.add_argument("--insecure-tls", action="store_true", help="skip MCP TLS verification")
    args = parser.parse_args()

    if args.self_check:
        self_check()
        return
    if args.demo:
        run_offline_demo()
        return
    if args.run_once:
        run_once(args)
        return
    serve_forever(args)


if __name__ == "__main__":
    main()
