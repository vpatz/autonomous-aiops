#!/usr/bin/env python3
"""Assemble the multi-agent workflow as a LangGraph StateGraph.

    START -> anomaly_detector
                 |  no anomaly -> END (healthy / remediation verified)
                 v
           root_cause_analyzer  <-- inner loop: iterates through MCP tools
                 |  root cause found
                 v
           remediation_selector  (approved commands from the Ansible MCP server)
                 v
           remediation_executor  -> anomaly_detector  (outer loop verification)

Nodes route with langgraph Command objects; `investigation` and `findings`
are append-only channels, so the full decision trail survives every loop.

Verify:

    python3 3_langgraph_workflow/graph.py --self-check
"""

import argparse

import anomaly_detector
import mcp_client
import remediation_selector
import root_cause_analyzer
import state
from state import WorkflowState


def build_graph(
    linux_mcp=None,
    ansible_mcp=None,
    get_window=None,
    use_llm=True,
    silence_seconds=state.DEFAULT_SILENCE_SECONDS,
    verify_wait_seconds=state.DEFAULT_VERIFY_WAIT_SECONDS,
    deliver=None,
):
    """Compile the workflow graph. Dependencies are injected for testability:
    real MCP clients and live windows in production, fakes in self-checks."""
    from langgraph.graph import END, START, StateGraph

    graph = StateGraph(WorkflowState)
    graph.add_node(state.NODE_DETECTOR, anomaly_detector.build_node(
        silence_seconds=silence_seconds, use_llm=use_llm))
    graph.add_node(state.NODE_RCA, root_cause_analyzer.build_node(
        linux_mcp=linux_mcp, use_llm=use_llm))
    graph.add_node(state.NODE_SELECTOR, remediation_selector.build_selector_node(
        ansible_mcp=ansible_mcp, use_llm=use_llm, eda=deliver is not None))
    graph.add_node(state.NODE_EXECUTOR, remediation_selector.build_executor_node(
        ansible_mcp=ansible_mcp, get_window=get_window, verify_wait_seconds=verify_wait_seconds,
        deliver=deliver))
    graph.add_edge(START, state.NODE_DETECTOR)
    # All other routing is dynamic: nodes return Command(goto=...), including
    # Command(goto=END) to finish.
    return graph.compile()


def run(app, workflow_state, label=None, print_steps=True):
    """Run the graph to completion, printing new findings as they appear.

    Returns the final state. Uses stream mode "values" so the demo shows the
    sequence of steps and iterations performed by the multi-agent workflow.
    """
    label = label or workflow_state.get("service_name", "workflow")
    config = {"recursion_limit": 20 + 10 * (
        workflow_state.get("max_iterations", state.DEFAULT_MAX_ITERATIONS)
        + workflow_state.get("max_attempts", state.DEFAULT_MAX_ATTEMPTS))}
    final = None
    seen = 0
    for snapshot in app.stream(workflow_state, config=config, stream_mode="values"):
        final = snapshot
        findings = snapshot.get("findings") or []
        if print_steps:
            for line in findings[seen:]:
                print(f"  [{label}] {line}", flush=True)
        seen = len(findings)
    return final


def _fake_window(anomaly_flag):
    """Window snapshot used by the self-check scenarios."""

    def window(service, seconds, since=None):
        if anomaly_flag["anomalous"]:
            # The chaos injector killed the service: it was seen, then went
            # silent (no metrics, logs or traces in the window).
            return {
                "ever_seen": True,
                "last_seen_seconds_ago": 42.0,
                "observed_for_seconds": 620.0,
                "metrics": {},
                "metric_events": [],
                "logs": [],
                "traces": [],
            }
        return {
            "ever_seen": True,
            "last_seen_seconds_ago": 1.0,
            "observed_for_seconds": 620.0,
            "metrics": {"service.uptime": 2.0},
            "metric_events": [],
            "logs": [],
            "traces": [],
        }

    return window


def self_check():
    import mock_mcp_server

    mock_mcp_server.reset()

    # Scenario A: anomaly -> RCA inner loop -> approved remediation ->
    # outer-loop verification sees healthy telemetry -> REMEDIATED.
    anomaly_flag = {"anomalous": True}
    linux = mcp_client.FakeMcpClient(mock_mcp_server.linux_tools(), mock_mcp_server.linux_call)
    ansible = mcp_client.FakeMcpClient(mock_mcp_server.ansible_tools(), mock_mcp_server.ansible_call)

    def get_window(service, seconds, since=None):
        # The mock ansible server flips to "restarted" once the executor runs,
        # which is when the service becomes healthy again.
        if mock_mcp_server.service_restarted():
            anomaly_flag["anomalous"] = False
        return _fake_window(anomaly_flag)(service, seconds, since)

    app = build_graph(
        linux_mcp=linux,
        ansible_mcp=ansible,
        get_window=get_window,
        use_llm=False,
        verify_wait_seconds=0,
    )
    telemetry = _fake_window(anomaly_flag)("vm-1", 120)
    final = run(app, state.initial_state("vm-1", telemetry=telemetry), print_steps=False)
    if final.get("status") != state.STATUS_REMEDIATED:
        raise SystemExit(f"graph: scenario A expected REMEDIATED, got {final.get('status')}\n"
                         f"{final.get('findings')}")
    if final.get("attempts") != 1:
        raise SystemExit(f"graph: scenario A attempts={final.get('attempts')}")
    if len(final.get("investigation", [])) < 2:
        raise SystemExit("graph: scenario A inner loop did not iterate")
    if not final.get("remediation_result", {}).get("ok"):
        raise SystemExit("graph: scenario A remediation did not succeed")
    if not final.get("root_cause", {}).get("found"):
        raise SystemExit("graph: scenario A root cause missing")

    # Scenario B: the service never recovers -> retries the full outer loop
    # until max_attempts, then reports FAILED.
    linux2 = mcp_client.FakeMcpClient(mock_mcp_server.linux_tools(), mock_mcp_server.linux_call)
    ansible2 = mcp_client.FakeMcpClient(mock_mcp_server.ansible_tools(), mock_mcp_server.ansible_call)
    stuck = {"anomalous": True}
    app = build_graph(
        linux_mcp=linux2,
        ansible_mcp=ansible2,
        get_window=_fake_window(stuck),
        use_llm=False,
        verify_wait_seconds=0,
    )
    final = run(
        app,
        state.initial_state("vm-1", telemetry=_fake_window(stuck)("vm-1", 120), max_attempts=2),
        print_steps=False,
    )
    if final.get("status") != state.STATUS_FAILED:
        raise SystemExit(f"graph: scenario B expected FAILED, got {final.get('status')}")
    if final.get("attempts") != 2:
        raise SystemExit(f"graph: scenario B attempts={final.get('attempts')}")

    # Scenario C: healthy service -> immediate END with status healthy.
    app = build_graph(use_llm=False)
    healthy = {"ever_seen": True, "last_seen_seconds_ago": 1.0,
               "metrics": {}, "logs": [], "traces": []}
    final = run(app, state.initial_state("vm-1", telemetry=healthy), print_steps=False)
    if final.get("status") != state.STATUS_HEALTHY:
        raise SystemExit(f"graph: scenario C expected HEALTHY, got {final.get('status')}")
    print("graph: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="LangGraph multi-agent workflow graph")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not args.self_check:
        raise SystemExit("pass --self-check; run workflow.py to execute the graph")
    self_check()


if __name__ == "__main__":
    main()
