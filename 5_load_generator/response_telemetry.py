"""Export one-second client observations as two-second OTLP/HTTP log batches."""

import json
import threading
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest

EXPORT_INTERVAL_SECONDS = 2


def encode_batch(targets, start, stop, counts):
    batch = ExportLogsServiceRequest()
    for vm in targets:
        resource = batch.resource_logs.add()
        for key, value in (("service.name", "aiops-load-generator"), ("target.vm", vm)):
            resource.resource.attributes.add(key=key).value.string_value = value
        scope = resource.scope_logs.add()
        scope.scope.name = "aiops.load_generator"
        for second in range(start, stop):
            outcomes = counts.get((second, vm), {})
            log = scope.log_records.add(time_unix_nano=second * 10**9, severity_text="INFO")
            log.body.string_value = json.dumps({
                "second": datetime.fromtimestamp(second, timezone.utc).isoformat(),
                "vm": vm, "responses": dict(outcomes),
            }, separators=(",", ":"))
    return batch.SerializeToString()


class ResponseTelemetry:
    def __init__(self, base_url, targets):
        self.url = base_url.rstrip("/") + "/v1/logs"
        self.targets = targets
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.counts = defaultdict(Counter)
        self.thread = threading.Thread(target=self.run, name="response-telemetry")

    def __enter__(self):
        self.first = int(time.time())
        self.thread.start()
        return self

    def record(self, vm, status):
        with self.lock:
            key = str(status) if status and 100 <= status <= 599 else "no_response"
            self.counts[int(time.time()), vm][key] += 1

    def flush(self, final=False):
        with self.lock:
            end = int(time.time()) + int(final)
            start = self.first
            if end <= start:
                return
            counts = {k: self.counts.pop(k) for k in list(self.counts) if k[0] < end}
            self.first = end
        data = encode_batch(self.targets, start, end, counts)
        req = urllib.request.Request(self.url, data=data, headers={"Content-Type": "application/x-protobuf"})
        try:
            with urllib.request.urlopen(req, timeout=3) as response:
                response.read()
        except OSError as exc:
            print(f"[load generator] telemetry batch {start}..{end-1} not delivered: {exc}", flush=True)

    def run(self):
        deadline = time.monotonic() + EXPORT_INTERVAL_SECONDS
        while not self.stop.wait(max(0, deadline - time.monotonic())):
            self.flush()
            deadline += EXPORT_INTERVAL_SECONDS

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()
        self.flush(final=True)
