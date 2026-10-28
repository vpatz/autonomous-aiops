#!/usr/bin/env python3
"""Step 1: receive OTLP/HTTP metrics.

Verify without a cluster:

    python3 2_aiops_agent/receive_otel.py --self-check

Listen for live metrics:

    python3 2_aiops_agent/receive_otel.py
"""

import gzip
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "eda"))

import receive_metrics
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)

LISTEN_HOST = receive_metrics.LISTEN_HOST
LISTEN_PORT = receive_metrics.LISTEN_PORT
STEP = "step 1 receive otel"


@dataclass
class MetricExport:
    service_name: str
    received_at: float
    metrics: dict = field(default_factory=dict)
    text: str = ""


class ExportStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}
        self._batch = []

    def record(self, export):
        with self._lock:
            self._latest[export.service_name] = export
            self._batch.append(export)

    def snapshot(self):
        with self._lock:
            return dict(self._latest)

    def take_batch(self):
        with self._lock:
            batch = self._batch
            self._batch = []
            return batch


def parse_otlp_body(payload, content_encoding=None, received_at=None):
    if content_encoding == "gzip":
        payload = gzip.decompress(payload)
    request = ExportMetricsServiceRequest()
    request.ParseFromString(payload)
    received_at = time.monotonic() if received_at is None else received_at
    text = receive_metrics._format_request(request)
    exports = []
    for resource_metrics in request.resource_metrics:
        service_name = receive_metrics._resource_name(resource_metrics.resource)
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
                value = receive_metrics._number(points[-1])
                if value is not None:
                    metrics[metric.name] = value
        exports.append(MetricExport(service_name, received_at, metrics, text))
    return exports


def log_received(export):
    print(
        f"{STEP}: service={export.service_name} metrics={len(export.metrics)}",
        flush=True,
    )
    if export.text:
        print(export.text, flush=True)


class OtlpHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    store = None

    def do_POST(self):
        if self.path.split("?", 1)[0] not in ("/v1/metrics", "/"):
            self.send_error(404, "expected /v1/metrics")
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length)
        exports = parse_otlp_body(payload, self.headers.get("Content-Encoding"))
        if self.store is not None:
            for export in exports:
                self.store.record(export)
        if not exports:
            print(f"{STEP}: empty metrics export", flush=True)
        else:
            for export in exports:
                log_received(export)
        body = ExportMetricsServiceResponse().SerializeToString()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def sample_payload():
    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    attribute = resource_metrics.resource.attributes.add()
    attribute.key = "service.name"
    attribute.value.string_value = "self-check"
    metric = resource_metrics.scope_metrics.add().metrics.add()
    metric.name = "service.uptime"
    point = metric.gauge.data_points.add()
    point.as_double = 1.5
    return request.SerializeToString()


def self_check():
    payload = sample_payload()
    exports = parse_otlp_body(payload, received_at=10.0)
    if len(exports) != 1:
        raise SystemExit(f"{STEP}: expected 1 export, got {len(exports)}")
    export = exports[0]
    if export.service_name != "self-check" or export.metrics.get("service.uptime") != 1.5:
        raise SystemExit(f"{STEP}: parsed export did not match {export}")
    store = ExportStore()
    store.record(export)
    if store.snapshot()["self-check"].metrics["service.uptime"] != 1.5:
        raise SystemExit(f"{STEP}: store did not keep the export")
    if store.take_batch()[0].service_name != "self-check" or store.take_batch():
        raise SystemExit(f"{STEP}: batch was not returned once")
    print(f"{STEP}: self-check ok", flush=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Step 1: receive OTLP/HTTP metrics")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--host", default=LISTEN_HOST)
    parser.add_argument("--port", type=int, default=LISTEN_PORT)
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    OtlpHandler.store = ExportStore()
    server = ThreadingHTTPServer((args.host, args.port), OtlpHandler)
    print(f"{STEP}: listening on {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"{STEP}: stopped", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
