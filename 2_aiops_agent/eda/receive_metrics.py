#!/usr/bin/env python3
"""Receive OTLP/HTTP metrics and print them.

The demo app on the VM pushes to http://192.168.64.1:4318/v1/metrics.
Run this on the Mac, from the project virtualenv:

    .venv/bin/python sample-workload/receive_metrics.py
"""

import gzip
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 4318

_TEMPORALITY = {0: "unspecified", 1: "delta", 2: "cumulative"}


def _any_value(value):
    kind = value.WhichOneof("value")
    if kind is None:
        return ""
    return getattr(value, kind)


def _attributes(pairs):
    rendered = []
    for pair in pairs:
        rendered.append(f"{pair.key}={_any_value(pair.value)}")
    if not rendered:
        return ""
    return "{" + ", ".join(rendered) + "}"


def _resource_name(resource):
    for pair in resource.attributes:
        if pair.key == "service.name":
            return str(_any_value(pair.value))
    return "unknown"


def _number(point):
    kind = point.WhichOneof("value")
    if kind == "as_int":
        return point.as_int
    if kind == "as_double":
        return point.as_double
    return None


def _histogram_summary(point):
    parts = [f"count={point.count}"]
    if point.HasField("sum"):
        parts.append(f"sum={point.sum}")
    return " ".join(parts)


def _format_request(request):
    lines = []
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for resource_metrics in request.resource_metrics:
        service = _resource_name(resource_metrics.resource)
        lines.append(f"--- {stamp}  service.name={service} ---")
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                kind = metric.WhichOneof("data")
                body = getattr(metric, kind) if kind else None
                temporality = ""
                if body is not None and hasattr(body, "aggregation_temporality"):
                    temporality = _TEMPORALITY.get(body.aggregation_temporality, "")
                suffix = f"  {temporality}" if temporality else ""
                if body is None:
                    lines.append(f"{metric.name}{suffix}")
                    continue
                for point in body.data_points:
                    attrs = _attributes(point.attributes)
                    if kind in ("gauge", "sum"):
                        value = _number(point)
                        lines.append(f"{metric.name}{attrs} = {value}{suffix}")
                    elif kind == "histogram":
                        lines.append(f"{metric.name}{attrs} {_histogram_summary(point)}{suffix}")
                    elif kind == "exponential_histogram":
                        lines.append(f"{metric.name}{attrs} count={point.count}{suffix}")
                    else:
                        lines.append(f"{metric.name}{attrs}{suffix}")
    return "\n".join(lines)


class OtlpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        if self.path.split("?", 1)[0] not in ("/v1/metrics", "/"):
            self.send_error(404, "expected /v1/metrics")
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length)
        if self.headers.get("Content-Encoding") == "gzip":
            payload = gzip.decompress(payload)
        request = ExportMetricsServiceRequest()
        request.ParseFromString(payload)
        text = _format_request(request)
        if text:
            print(text, flush=True)
        else:
            print("received an empty metrics export", flush=True)
        body = ExportMetricsServiceResponse().SerializeToString()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def main():
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), OtlpHandler)
    print(f"listening on {LISTEN_HOST}:{LISTEN_PORT} for OTLP/HTTP metrics", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("stopped", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
