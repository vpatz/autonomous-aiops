#!/usr/bin/env python3
"""Run the three AIOps agent steps.

1. receive_otel.py receives OTLP/HTTP metrics.
2. analyze_metrics.py marks a service down when those metrics stop.
3. call_eda.py posts a restart event for that service.

Verify each step, then the pipeline:

    python3 2_aiops_agent/receive_otel.py --self-check
    python3 2_aiops_agent/analyze_metrics.py --self-check
    python3 2_aiops_agent/call_eda.py --dry-run
    python3 2_aiops_agent/aiops_agent.py --self-check
"""

import html
import sys
import threading
import time
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

from http.server import ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parent))

import analyze_metrics
import call_eda
import receive_otel
from receive_otel import LISTEN_HOST, LISTEN_PORT

SILENCE_SECONDS = analyze_metrics.SILENCE_SECONDS
COOLDOWN_SECONDS = analyze_metrics.COOLDOWN_SECONDS
DASHBOARD_PATHS = ("/", "/dashboard")


class StatusBoard:
    def __init__(self):
        self._lock = threading.Lock()
        self.last_attempts = {}
        self.eda = {}

    def attempts(self):
        with self._lock:
            return dict(self.last_attempts)

    def note_attempt(self, service_name):
        with self._lock:
            self.last_attempts[service_name] = time.monotonic()

    def record_eda(self, service_name, action, result):
        with self._lock:
            previous = self.eda.get(service_name, {})
            self.eda[service_name] = {
                "called": True,
                "action": action,
                "result": result,
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "count": previous.get("count", 0) + 1,
            }

    def eda_snapshot(self):
        with self._lock:
            return {name: dict(record) for name, record in self.eda.items()}


def render_dashboard(store, board, silence_seconds, cooldown_seconds):
    exports = store.snapshot()
    decisions = {
        decision.service_name: decision
        for decision in analyze_metrics.analyze(
            exports,
            time.monotonic(),
            silence_seconds,
            cooldown_seconds,
            board.attempts(),
        )
    }
    eda = board.eda_snapshot()
    rows = []
    for service_name in sorted(exports):
        export = exports[service_name]
        decision = decisions[service_name]
        metrics = ", ".join(
            f"{html.escape(str(name))}={html.escape(str(value))}"
            for name, value in sorted(export.metrics.items())
        ) or "none"
        call = eda.get(service_name)
        if call is None:
            eda_cell = "not called"
        else:
            eda_cell = (
                f"called {html.escape(call['action'])} "
                f"({html.escape(call['result'])}) at {html.escape(call['at'])}, "
                f"count {call['count']}"
            )
        rows.append(
            "<tr>"
            f"<td>{html.escape(service_name)}</td>"
            f"<td>{metrics}</td>"
            f"<td>{decision.silent_for:.0f}s ago</td>"
            f"<td>{html.escape(decision.status)}: {html.escape(decision.reason)}</td>"
            f"<td>{eda_cell}</td>"
            "</tr>"
        )
    body = (
        "<tr><td colspan=\"5\">No metrics collected yet.</td></tr>"
        if not rows
        else "\n".join(rows)
    )
    up = sum(1 for decision in decisions.values() if decision.status == "up")
    down = sum(1 for decision in decisions.values() if decision.status == "down")
    called = sum(1 for record in eda.values() if record.get("called"))
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>AIOps agent</title>
  <style>
    body {{ font-family: "Segoe UI", Helvetica, Arial, sans-serif; margin: 2rem; background: #f4f6f8; color: #1f2933; }}
    main {{ max-width: 64rem; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border: 1px solid #e2e8f0; padding: 0.6rem 0.8rem; text-align: left; vertical-align: top; }}
    th {{ background: #eef2f6; }}
  </style>
</head>
<body>
  <main>
    <h1>AIOps agent</h1>
    <p>{len(exports)} services, {up} up, {down} down, EDA called for {called}.</p>
    <table>
      <thead>
        <tr>
          <th>Service</th>
          <th>Collected metrics</th>
          <th>Last metric</th>
          <th>Analysis</th>
          <th>EDA</th>
        </tr>
      </thead>
      <tbody>
        {body}
      </tbody>
    </table>
  </main>
</body>
</html>
"""


class AgentHandler(receive_otel.OtlpHandler):
    silence_seconds = SILENCE_SECONDS
    cooldown_seconds = COOLDOWN_SECONDS
    board = None

    def do_GET(self):
        if self.path.split("?", 1)[0] not in DASHBOARD_PATHS:
            self.send_error(404, "expected /dashboard")
            return
        page = render_dashboard(
            self.store,
            self.board,
            self.silence_seconds,
            self.cooldown_seconds,
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(page)))
        self.end_headers()
        self.wfile.write(page)

    def do_POST(self):
        super().do_POST()
        if self.path.split("?", 1)[0] not in ("/v1/metrics", "/"):
            return
        now = time.monotonic()
        for export in self.store.take_batch():
            decision = analyze_metrics.analyze(
                {export.service_name: export},
                now,
                self.silence_seconds,
                self.cooldown_seconds,
                self.board.attempts(),
            )[0]
            analyze_metrics.log_analysis(decision)


def watch_metrics(store, board, stop, url, username, password, silence_seconds, cooldown_seconds):
    last_status = {}
    while not stop.wait(1):
        decisions = analyze_metrics.analyze(
            store.snapshot(),
            time.monotonic(),
            silence_seconds,
            cooldown_seconds,
            board.attempts(),
        )
        for decision in decisions:
            previous = last_status.get(decision.service_name)
            last_status[decision.service_name] = decision.status
            if decision.status != "down":
                continue
            if previous != "down" or decision.action:
                analyze_metrics.log_analysis(decision)
            if not decision.action:
                continue
            board.note_attempt(decision.service_name)
            try:
                _status, _body = call_eda.call_eda(
                    url,
                    username,
                    password,
                    decision.action,
                    decision.service_name,
                )
            except (OSError, urllib.error.URLError) as exc:
                result = f"failed {exc}"
                print(f"{call_eda.STEP}: {result}", flush=True)
            else:
                result = f"accepted HTTP {_status}"
            board.record_eda(decision.service_name, decision.action, result)


def self_check():
    receive_otel.self_check()
    analyze_metrics.self_check()
    call_eda.self_check()
    store = receive_otel.ExportStore()
    store.record(
        receive_otel.MetricExport(
            "rhel10-service-1",
            time.monotonic(),
            {"service.uptime": 3.0, "service.cpu.load": 1.2},
        )
    )
    board = StatusBoard()
    page = render_dashboard(store, board, SILENCE_SECONDS, COOLDOWN_SECONDS)
    for expected in ("rhel10-service-1", "service.uptime", "up", "not called"):
        if expected not in page:
            raise SystemExit(f"dashboard missing {expected}")
    board.record_eda("rhel10-service-1", "restart", "accepted HTTP 202")
    called = render_dashboard(store, board, SILENCE_SECONDS, COOLDOWN_SECONDS)
    if "called restart" not in called or "accepted HTTP 202" not in called:
        raise SystemExit("dashboard did not show the EDA call")
    print("aiops_agent: steps 1, 2, and 3 ok", flush=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Receive metrics, analyze them, and call EDA")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--url", default=call_eda.DEFAULT_URL)
    parser.add_argument("--username", default=call_eda.DEFAULT_USERNAME)
    parser.add_argument("--password", default=call_eda.DEFAULT_PASSWORD)
    parser.add_argument("--silence-seconds", type=float, default=SILENCE_SECONDS)
    parser.add_argument("--cooldown-seconds", type=float, default=COOLDOWN_SECONDS)
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    if not args.password:
        raise SystemExit("Set EDA_EVENT_PASSWORD or pass --password")

    store = receive_otel.ExportStore()
    board = StatusBoard()
    AgentHandler.store = store
    AgentHandler.board = board
    AgentHandler.silence_seconds = args.silence_seconds
    AgentHandler.cooldown_seconds = args.cooldown_seconds
    stop = threading.Event()
    watcher = threading.Thread(
        target=watch_metrics,
        args=(
            store,
            board,
            stop,
            args.url,
            args.username,
            args.password,
            args.silence_seconds,
            args.cooldown_seconds,
        ),
        daemon=True,
    )
    watcher.start()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), AgentHandler)
    print(
        f"aiops_agent: {receive_otel.STEP}; {analyze_metrics.STEP}; {call_eda.STEP}; "
        f"listening on {LISTEN_HOST}:{LISTEN_PORT}; dashboard at /dashboard",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("aiops_agent: stopped", flush=True)
    finally:
        stop.set()
        server.server_close()


if __name__ == "__main__":
    main()
