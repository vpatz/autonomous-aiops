#!/usr/bin/env python3
"""Telemetry collector: receive OTLP/HTTP metrics, logs, and traces.

The demo service in each VM pushes all three signals here. Every record is
kept with a monotonic receive timestamp so the workflow can query time
windows (and post-remediation windows) per service.

Endpoints on :4318:

    POST /v1/metrics  POST /v1/logs  POST /v1/traces   (OTLP/HTTP protobuf)
    GET  /            collector dashboard
    GET  /windows     JSON windows: ?service=NAME&seconds=120&since=MONO

Verify without a cluster:

    python3 3_langgraph_workflow/collector.py --self-check

Listen for live telemetry (prints everything it receives):

    python3 3_langgraph_workflow/collector.py
"""

import argparse
import gzip
import html
import json
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

STEP = "telemetry collector"
LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 4318
ERROR_SEVERITY_NUMBER = 17  # OTel SeverityNumbers: 17=ERROR, 18..24 = ERROR2..FATAL
MAX_PER_SIGNAL = 200
# Metrics whose increases are recorded as fault events. OTel counters are
# cumulative, so "dummy-crash is present" is not a fault signal — the fault
# is the increment. A restart resets the counter and clears the fault.
EVENT_METRICS = ("dummy-crash",)
# Metrics whose decreases indicate the emitting process restarted
# (a fresh service.uptime below the previous sample).
RESTART_METRICS = ("service.uptime",)


def _resource_name(resource):
    for pair in resource.attributes:
        if pair.key == "service.name":
            kind = pair.value.WhichOneof("value")
            return str(getattr(pair.value, kind)) if kind else "unknown"
    return "unknown"


def _number(point):
    kind = point.WhichOneof("value")
    if kind == "as_int":
        return point.as_int
    if kind == "as_double":
        return point.as_double
    return None


def _maybe_gzip(payload, content_encoding):
    if content_encoding == "gzip":
        return gzip.decompress(payload)
    return payload


def parse_metrics(payload, content_encoding=None, received_at=None, retain=None):
    """Parse an OTLP metrics export into [(service_name, {metric: value})]."""
    request = ExportMetricsServiceRequest()
    request.ParseFromString(_maybe_gzip(payload, content_encoding))
    received_at = time.monotonic() if received_at is None else received_at
    parsed = []
    for resource_metrics in request.resource_metrics:
        service_name = _resource_name(resource_metrics.resource)
        if retain is not None:
            retain(service_name, "metrics", resource_metrics, received_at)
        metrics = {}
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                kind = metric.WhichOneof("data")
                body = getattr(metric, kind) if kind else None
                if body is None or kind not in ("gauge", "sum"):
                    continue
                points = list(body.data_points)
                if not points:
                    continue
                value = _number(points[-1])
                if value is not None:
                    metrics[metric.name] = value
        parsed.append((service_name, metrics, received_at))
    return parsed


def parse_logs(payload, content_encoding=None, received_at=None, retain=None):
    """Parse an OTLP logs export into [(service_name, [log entry dicts])]."""
    request = ExportLogsServiceRequest()
    request.ParseFromString(_maybe_gzip(payload, content_encoding))
    received_at = time.monotonic() if received_at is None else received_at
    parsed = []
    for resource_logs in request.resource_logs:
        service_name = _resource_name(resource_logs.resource)
        if retain is not None:
            retain(service_name, "logs", resource_logs, received_at)
        entries = []
        for scope_logs in resource_logs.scope_logs:
            for record in scope_logs.log_records:
                body_kind = record.body.WhichOneof("value")
                entries.append(
                    {
                        "severity_number": int(record.severity_number),
                        "severity_text": record.severity_text,
                        "body": str(getattr(record.body, body_kind)) if body_kind else "",
                        "at": received_at,
                    }
                )
        parsed.append((service_name, entries, received_at))
    return parsed


def parse_traces(payload, content_encoding=None, received_at=None, retain=None):
    """Parse an OTLP traces export into [(service_name, [span dicts])]."""
    request = ExportTraceServiceRequest()
    request.ParseFromString(_maybe_gzip(payload, content_encoding))
    received_at = time.monotonic() if received_at is None else received_at
    parsed = []
    for resource_spans in request.resource_spans:
        service_name = _resource_name(resource_spans.resource)
        if retain is not None:
            retain(service_name, "traces", resource_spans, received_at)
        spans = []
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                duration_ms = 0.0
                if span.end_time_unix_nano and span.start_time_unix_nano:
                    duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1e6
                spans.append(
                    {
                        "name": span.name,
                        "status_code": int(span.status.code),
                        "status_message": span.status.message,
                        "duration_ms": round(duration_ms, 3),
                        "at": received_at,
                    }
                )
        parsed.append((service_name, spans, received_at))
    return parsed


def _short(value):
    if isinstance(value, float):
        return f"{value:.1f}"
    return str(value)


def _metrics_message(metrics):
    """One compact line describing a received metrics export."""
    parts = []
    for name, value in sorted(metrics.items()):
        if name.startswith("otel.sdk"):
            continue  # SDK-internal queue metrics are noise on a message feed
        parts.append(f"{name}={_short(value)}")
    return "metrics: " + (", ".join(parts) or "(empty)")


class TelemetryStore:
    """Per-service rolling telemetry buffers with time-window queries."""

    def __init__(self):
        self._lock = threading.Lock()
        self._services = {}
        # Full protobuf resources for anomaly-model evidence. Time buckets keep
        # retention independent of traffic volume and the dashboard's deque caps.
        self._exports = {}

    def _expire_exports(self, cutoff):
        for second in list(self._exports):
            if second < int(cutoff):
                del self._exports[second]

    def retain_export(self, service, signal, resource, received_at):
        with self._lock:
            cutoff = time.monotonic() - 30
            self._expire_exports(cutoff)
            if received_at >= cutoff:
                self._exports.setdefault(int(received_at), []).append(
                    (received_at, service, signal, resource))

    def recent_evidence(self, service, since=0):
        """All resource exports for one VM in receipt order; convert outside the lock."""
        from google.protobuf.json_format import MessageToDict

        now = time.monotonic()
        with self._lock:
            self._expire_exports(now - 30)
            cutoff = max(now - 30, since)
            records = [r for bucket in self._exports.values() for r in bucket
                       if r[1] == service and cutoff <= r[0] <= now]
        return {f"{signal}:{i}": {"age_seconds": round(now - at, 3),
                                  "otlp": MessageToDict(resource)}
                for i, (at, _, signal, resource) in enumerate(sorted(records, key=lambda r: r[0]))}

    def _entry(self, service_name):
        if service_name not in self._services:
            self._services[service_name] = {
                "first_seen": time.monotonic(),
                "last_seen": time.monotonic(),
                "metrics": {},  # name -> {"value": float, "at": monotonic}
                "metric_events": deque(maxlen=MAX_PER_SIGNAL),
                "logs": deque(maxlen=MAX_PER_SIGNAL),
                "traces": deque(maxlen=MAX_PER_SIGNAL),
                "messages": deque(maxlen=MAX_PER_SIGNAL),
            }
        return self._services[service_name]

    def _message(self, entry, text, received_at):
        entry["messages"].append({"text": text, "at": received_at})

    def record_metrics(self, parsed):
        with self._lock:
            for service_name, metrics, received_at in parsed:
                entry = self._entry(service_name)
                entry["last_seen"] = max(entry["last_seen"], received_at)
                self._message(entry, _metrics_message(metrics), received_at)
                for name, value in metrics.items():
                    previous = entry["metrics"].get(name, {}).get("value")
                    entry["metrics"][name] = {"value": value, "at": received_at}
                    if name in EVENT_METRICS:
                        increased = value > previous if previous is not None else value > 0
                        if increased:
                            entry["metric_events"].append(
                                {"metric": name, "from": previous or 0, "to": value,
                                 "at": received_at, "kind": "increment"}
                            )
                    elif name in RESTART_METRICS and previous is not None and value < previous:
                        entry["metric_events"].append(
                            {"metric": name, "from": previous, "to": value,
                             "at": received_at, "kind": "restart"}
                        )

    def record_logs(self, parsed):
        with self._lock:
            for service_name, entries, received_at in parsed:
                entry = self._entry(service_name)
                entry["last_seen"] = max(entry["last_seen"], received_at)
                entry["logs"].extend(entries)
                for item in entries:
                    severity = item.get("severity_text") or f"sev{item.get('severity_number', 0)}"
                    self._message(entry, f"log {severity}: {item.get('body', '')}", received_at)

    def record_traces(self, parsed):
        with self._lock:
            for service_name, spans, received_at in parsed:
                entry = self._entry(service_name)
                entry["last_seen"] = max(entry["last_seen"], received_at)
                entry["traces"].extend(spans)
                for span in spans:
                    status = {0: "unset", 1: "ok", 2: "error"}.get(span["status_code"], "?")
                    self._message(
                        entry,
                        f"span {span['name']}: status={status} {span['duration_ms']}ms",
                        received_at,
                    )

    def messages(self, service_name, seconds=30):
        """The last `seconds` of received-message lines for one service."""
        now = time.monotonic()
        cutoff = now - max(0, seconds)
        with self._lock:
            entry = self._services.get(service_name)
            if entry is None:
                return []
            return [
                {"age_seconds": round(now - item["at"], 1), "text": item["text"]}
                for item in entry["messages"]
                if item["at"] >= cutoff
            ]

    def services(self):
        with self._lock:
            return sorted(self._services)

    def window(self, service_name, seconds, since=None):
        """Snapshot of the last `seconds` of telemetry for one service.

        `since` (monotonic seconds) further restricts records to those
        received at or after a point in time — used to verify remediation
        against only post-remediation telemetry.
        """
        now = time.monotonic()
        cutoff = now - max(0, seconds)
        if since is not None:
            cutoff = max(cutoff, since)
        with self._lock:
            entry = self._services.get(service_name)
            if entry is None:
                return {
                    "ever_seen": False,
                    "last_seen_seconds_ago": None,
                    "observed_for_seconds": None,
                    "metrics": {},
                    "metric_events": [],
                    "logs": [],
                    "traces": [],
                }
            metrics = {
                name: sample["value"]
                for name, sample in entry["metrics"].items()
                if sample["at"] >= cutoff
            }
            metric_events = [
                {
                    "metric": item["metric"],
                    "from": item["from"],
                    "to": item["to"],
                    "kind": item.get("kind", "increment"),
                    "age_seconds": round(now - item["at"], 1),
                }
                for item in entry["metric_events"]
                if item["at"] >= cutoff
            ]
            logs = [
                {
                    "severity_number": item["severity_number"],
                    "severity_text": item["severity_text"],
                    "body": item["body"],
                    "age_seconds": round(now - item["at"], 1),
                }
                for item in entry["logs"]
                if item["at"] >= cutoff
            ]
            traces = [
                {
                    "name": item["name"],
                    "status_code": item["status_code"],
                    "status_message": item["status_message"],
                    "duration_ms": item["duration_ms"],
                    "age_seconds": round(now - item["at"], 1),
                }
                for item in entry["traces"]
                if item["at"] >= cutoff
            ]
            return {
                "ever_seen": True,
                "last_seen_seconds_ago": round(now - entry["last_seen"], 1),
                "observed_for_seconds": round(now - entry["first_seen"], 1),
                "metrics": metrics,
                "metric_events": metric_events,
                "logs": logs,
                "traces": traces,
            }


def sample_metrics_payload(service_name="self-check", extra_metrics=None):
    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    attribute = resource_metrics.resource.attributes.add()
    attribute.key = "service.name"
    attribute.value.string_value = service_name
    metric = resource_metrics.scope_metrics.add().metrics.add()
    metric.name = "service.uptime"
    point = metric.gauge.data_points.add()
    point.as_double = 1.5
    for name, value in (extra_metrics or {}).items():
        extra = resource_metrics.scope_metrics.add().metrics.add()
        extra.name = name
        extra_point = extra.gauge.data_points.add()
        extra_point.as_double = float(value)
    return request.SerializeToString()


def sample_logs_payload(service_name="self-check", body="simulated a dummy crash"):
    request = ExportLogsServiceRequest()
    resource_logs = request.resource_logs.add()
    attribute = resource_logs.resource.attributes.add()
    attribute.key = "service.name"
    attribute.value.string_value = service_name
    record = resource_logs.scope_logs.add().log_records.add()
    record.severity_number = 17
    record.severity_text = "ERROR"
    record.body.string_value = body
    return request.SerializeToString()


def sample_traces_payload(service_name="self-check", span_name="dummy-crash"):
    request = ExportTraceServiceRequest()
    resource_spans = request.resource_spans.add()
    attribute = resource_spans.resource.attributes.add()
    attribute.key = "service.name"
    attribute.value.string_value = service_name
    span = resource_spans.scope_spans.add().spans.add()
    span.name = span_name
    span.status.code = 2  # ERROR
    span.status.message = "dummy crash"
    span.start_time_unix_nano = 1_000_000_000
    span.end_time_unix_nano = 1_005_000_000
    return request.SerializeToString()


def format_parsed(parsed_metrics=(), parsed_logs=(), parsed_traces=()):
    lines = []
    for service_name, metrics, _ in parsed_metrics:
        rendered = ", ".join(f"{name}={value}" for name, value in sorted(metrics.items()))
        lines.append(f"{STEP}: metrics service={service_name} {rendered or '(empty)'}")
    for service_name, entries, _ in parsed_logs:
        for item in entries:
            lines.append(
                f"{STEP}: log service={service_name} {item['severity_text']} {item['body']}"
            )
    for service_name, spans, _ in parsed_traces:
        for span in spans:
            status = {0: "unset", 1: "ok", 2: "error"}.get(span["status_code"], "?")
            lines.append(
                f"{STEP}: span service={service_name} {span['name']} status={status} "
                f"{span['duration_ms']}ms"
            )
    return lines


class CollectorHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    store = None
    verbose = True

    def _respond_proto(self, body):
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length)
        encoding = self.headers.get("Content-Encoding")
        if path in ("/v1/metrics", "/"):
            parsed = parse_metrics(payload, encoding, retain=self.store.retain_export)
            self.store.record_metrics(parsed)
            response = ExportMetricsServiceResponse().SerializeToString()
        elif path == "/v1/logs":
            parsed = parse_logs(payload, encoding, retain=self.store.retain_export)
            self.store.record_logs(parsed)
            response = ExportLogsServiceResponse().SerializeToString()
        elif path == "/v1/traces":
            parsed = parse_traces(payload, encoding, retain=self.store.retain_export)
            self.store.record_traces(parsed)
            response = ExportTraceServiceResponse().SerializeToString()
        else:
            self.send_error(404, "expected /v1/metrics, /v1/logs or /v1/traces")
            return
        if self.verbose:
            for line in format_parsed(
                parsed if path in ("/v1/metrics", "/") else (),
                parsed if path == "/v1/logs" else (),
                parsed if path == "/v1/traces" else (),
            ):
                print(line, flush=True)
        self._respond_proto(response)

    def do_GET(self):
        from urllib.parse import parse_qs, urlparse

        parsed_url = urlparse(self.path)
        if parsed_url.path == "/windows":
            query = parse_qs(parsed_url.query)
            seconds = float(query.get("seconds", ["120"])[0])
            since = query.get("since", [None])[0]
            service = query.get("service", [None])[0]
            if service:
                names = [service]
            else:
                names = self.store.services()
            payload = {
                name: self.store.window(name, seconds, since=float(since) if since else None)
                for name in names
            }
            body = json.dumps(payload, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed_url.path == "/":
            page = render_dashboard(self.store).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        self.send_error(404, "expected / or /windows")

    def log_message(self, fmt, *args):
        return


def render_dashboard(store, silence_seconds=10):
    rows = []
    now = time.monotonic()
    with store._lock:
        for name in sorted(store._services):
            entry = store._services[name]
            last_seen = now - entry["last_seen"]
            status = "up" if last_seen < silence_seconds else "silent"
            error_logs = sum(
                1 for item in entry["logs"] if item["severity_number"] >= ERROR_SEVERITY_NUMBER
            )
            failed_spans = sum(1 for span in entry["traces"] if span["status_code"] == 2)
            rows.append(
                "<tr>"
                f"<td>{html.escape(name)}</td>"
                f"<td>{len(entry['metrics'])}</td>"
                f"<td>{len(entry['logs'])} ({error_logs} errors)</td>"
                f"<td>{len(entry['traces'])} ({failed_spans} failed)</td>"
                f"<td>{last_seen:.0f}s ago</td>"
                f"<td>{status}</td>"
                "</tr>"
            )
    body = (
        "<tr><td colspan=\"6\">No telemetry collected yet.</td></tr>"
        if not rows
        else "\n".join(rows)
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="5">
  <title>Telemetry collector</title>
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
    <h1>Telemetry collector</h1>
    <p>{len(rows)} services. OTLP/HTTP metrics, logs and traces on port {LISTEN_PORT}.</p>
    <table>
      <thead>
        <tr><th>Service</th><th>Metrics</th><th>Logs</th><th>Traces</th><th>Last telemetry</th><th>Status</th></tr>
      </thead>
      <tbody>
        {body}
      </tbody>
    </table>
  </main>
</body>
</html>
"""


def self_check():
    store = TelemetryStore()
    now = time.monotonic()
    metrics = parse_metrics(sample_metrics_payload("vm-1"), received_at=now - 30)
    if metrics[0][1].get("service.uptime") != 1.5:
        raise SystemExit(f"{STEP}: metrics parse failed {metrics}")
    store.record_metrics(metrics)
    store.record_logs(parse_logs(sample_logs_payload("vm-1"), received_at=now - 30))
    store.record_traces(parse_traces(sample_traces_payload("vm-1"), received_at=now - 30))
    # Counter increments: the fault event is the increase, not the presence.
    store.record_metrics(parse_metrics(
        sample_metrics_payload("vm-1", {"dummy-crash": 0}), received_at=now - 25))
    store.record_metrics(parse_metrics(
        sample_metrics_payload("vm-1", {"dummy-crash": 1, "service.uptime": 26.0}), received_at=now - 20))
    store.record_metrics(parse_metrics(
        sample_metrics_payload("vm-1", {"dummy-crash": 1, "service.uptime": 31.0}), received_at=now - 10))
    window = store.window("vm-1", 60)
    if not window["ever_seen"] or window["metrics"].get("service.uptime") != 31.0:
        raise SystemExit(f"{STEP}: window missing latest metrics {window}")
    if len(window["metric_events"]) != 1 or window["metric_events"][0]["to"] != 1:
        raise SystemExit(f"{STEP}: dummy-crash increment not recorded {window['metric_events']}")
    if window["metrics"].get("dummy-crash") != 1:
        raise SystemExit(f"{STEP}: raw counter value missing {window['metrics']}")
    if len(window["logs"]) != 1 or window["logs"][0]["severity_number"] != 17:
        raise SystemExit(f"{STEP}: window missing log {window['logs']}")
    if len(window["traces"]) != 1 or window["traces"][0]["status_code"] != 2:
        raise SystemExit(f"{STEP}: window missing trace {window['traces']}")
    if window["traces"][0]["duration_ms"] != 5.0:
        raise SystemExit(f"{STEP}: span duration wrong {window['traces'][0]}")
    # A short window must exclude old records but keep the service visible.
    short = store.window("vm-1", 5)
    if short["metrics"] or short["logs"] or short["traces"] or short["metric_events"]:
        raise SystemExit(f"{STEP}: short window should be empty {short}")
    if not short["ever_seen"]:
        raise SystemExit(f"{STEP}: short window should still see the service")
    # Post-remediation windows only include records after `since`.
    since = now - 15
    fresh = store.window("vm-1", 60, since=since)
    if fresh["logs"] or fresh["traces"] or fresh["metric_events"]:
        raise SystemExit(f"{STEP}: since filter should drop pre-since records {fresh}")
    if fresh["metrics"].get("dummy-crash") is None:
        raise SystemExit(f"{STEP}: since filter kept wrong metrics {fresh['metrics']}")
    unknown = store.window("never-seen", 60)
    if unknown["ever_seen"] or unknown["last_seen_seconds_ago"] is not None:
        raise SystemExit(f"{STEP}: unknown service window wrong {unknown}")
    # First observation of a nonzero counter counts as an increment
    # (collector restarted after the fault fired).
    store2 = TelemetryStore()
    store2.record_metrics(parse_metrics(
        sample_metrics_payload("vm-2", {"dummy-crash": 1}), received_at=time.monotonic()))
    if len(store2.window("vm-2", 60)["metric_events"]) != 1:
        raise SystemExit(f"{STEP}: first nonzero observation should be an event")
    # A decreasing service.uptime means the emitting process restarted.
    store.record_metrics(parse_metrics(
        sample_metrics_payload("vm-1", {"service.uptime": 100.0}), received_at=now - 8))
    store.record_metrics(parse_metrics(
        sample_metrics_payload("vm-1", {"service.uptime": 3.0}), received_at=now - 5))
    events = store.window("vm-1", 60)["metric_events"]
    restarts = [item for item in events if item.get("kind") == "restart"]
    if len(restarts) != 1 or restarts[0]["to"] != 3.0:
        raise SystemExit(f"{STEP}: uptime reset not recorded as a restart: {events}")
    if store.window("vm-1", 60).get("observed_for_seconds") is None:
        raise SystemExit(f"{STEP}: window missing observed_for_seconds")
    # The message feed: one line per received export, filtered by age.
    msgs = store.messages("vm-1", 60)
    if not msgs or not any(m["text"].startswith("metrics:") and "service.uptime" in m["text"]
                           for m in msgs):
        raise SystemExit(f"{STEP}: metrics message missing: {msgs[:3]}")
    if not any(m["text"].startswith("log ERROR:") for m in msgs):
        raise SystemExit(f"{STEP}: log message missing: {msgs[:3]}")
    if not any(m["text"].startswith("span dummy-crash:") and "status=error" in m["text"]
               for m in msgs):
        raise SystemExit(f"{STEP}: span message missing: {msgs[:3]}")
    if any("otel.sdk" in m["text"] for m in msgs):
        raise SystemExit(f"{STEP}: sdk-internal metrics leak into the feed: {msgs[:3]}")
    recent = store.messages("vm-1", 6)
    if len(recent) != 1 or "service.uptime=3.0" not in recent[0]["text"]:
        raise SystemExit(f"{STEP}: message age filter wrong: {recent}")
    if store.messages("never-seen", 60):
        raise SystemExit(f"{STEP}: unknown service must have no messages")
    print(f"{STEP}: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Receive OTLP/HTTP telemetry")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--host", default=LISTEN_HOST)
    parser.add_argument("--port", type=int, default=LISTEN_PORT)
    parser.add_argument("--quiet", action="store_true", help="do not print received telemetry")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    CollectorHandler.store = TelemetryStore()
    CollectorHandler.verbose = not args.quiet
    server = ThreadingHTTPServer((args.host, args.port), CollectorHandler)
    print(f"{STEP}: listening on {args.host}:{args.port} (metrics, logs, traces)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"{STEP}: stopped", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
