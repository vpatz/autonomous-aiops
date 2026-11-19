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


def render_workflow_dashboard(store, runs, runs_lock):
    with runs_lock:
        runs_snapshot = dict(runs)
    rows = []
    for service_name in sorted(store.services() or runs_snapshot):
        run = runs_snapshot.get(service_name)
        if run is None:
            status, detail = "no run yet", "waiting for an anomaly"
        elif run.get("running"):
            status, detail = "running", "multi-agent workflow in progress"
        else:
            status = run.get("status", "?")
            anomaly = run.get("anomaly", {})
            root_cause = run.get("root_cause", {})
            detail = root_cause.get("summary") or anomaly.get("summary") or ""
        rows.append(
            "<tr>"
            f"<td>{html.escape(service_name)}</td>"
            f"<td>{html.escape(str(status))}</td>"
            f"<td>{html.escape(str(detail)[:300])}</td>"
            "</tr>"
        )
    body = ("\n".join(rows)) or "<tr><td colspan=\"3\">No telemetry yet.</td></tr>"
    findings_rows = []
    for service_name in sorted(runs_snapshot):
        run = runs_snapshot[service_name]
        if not run or run.get("running"):
            continue
        for line in run.get("findings", [])[-12:]:
            findings_rows.append(
                f"<tr><td>{html.escape(service_name)}</td><td>{html.escape(line)}</td></tr>"
            )
    findings_body = ("\n".join(findings_rows)) or "<tr><td colspan=\"2\">No agent findings yet.</td></tr>"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>AIOps multi-agent workflow</title>
  <style>
    body {{ font-family: "Segoe UI", Helvetica, Arial, sans-serif; margin: 2rem; background: #f4f6f8; color: #1f2933; }}
    main {{ max-width: 72rem; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; margin-bottom: 2rem; }}
    th, td {{ border: 1px solid #e2e8f0; padding: 0.6rem 0.8rem; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f6; }}
    td:nth-child(2) {{ white-space: nowrap; }}
  </style>
</head>
<body>
  <main>
    <h1>AIOps multi-agent workflow</h1>
    <p>collector: {len(store.services())} services &middot; llm: {"on" if llm.available() else "off (heuristic fallback)"}</p>
    <table>
      <thead><tr><th>Service</th><th>Status</th><th>Detail</th></tr></thead>
      <tbody>{body}</tbody>
    </table>
    <h2>Agent findings (latest run, newest last)</h2>
    <table>
      <thead><tr><th>Service</th><th>Finding</th></tr></thead>
      <tbody>{findings_body}</tbody>
    </table>
  </main>
</body>
</html>
"""


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
