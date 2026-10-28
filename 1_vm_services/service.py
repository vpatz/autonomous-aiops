"""AIOps demo app.

GET /        page with a timer that starts when the page loads
GET /health  whether this process is serving requests

Metrics are pushed over OTLP/HTTP to OTEL_EXPORTER_OTLP_ENDPOINT
(default http://192.168.64.1:4318) every OTEL_METRIC_EXPORT_INTERVAL ms.
Host CPU and memory load are demo.cpu.load and demo.memory.load.
The process exits at a random time within 5 minutes of start so the service goes down.
"""

import atexit
import os
import random
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string, request
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

os.environ.setdefault("OTEL_SERVICE_NAME", "aiops-demo-app")
os.environ.setdefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://192.168.64.1:4318")
os.environ.setdefault("OTEL_METRIC_EXPORT_INTERVAL", "5000")

app = Flask(__name__)
STARTED_AT = time.time()


def _parent_command(pid):
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as handle:
            raw = handle.read()
    except OSError:
        return ""
    return raw.replace(b"\x00", b" ").decode(errors="replace")


def _crash_process():
    delay = random.uniform(0, 5 * 60)
    print(f"demo app will crash in {delay:.1f}s", file=sys.stderr, flush=True)
    time.sleep(delay)
    print("demo app crashing", file=sys.stderr, flush=True)
    parent = os.getppid()
    if parent > 1 and "gunicorn" in _parent_command(parent):
        os.kill(parent, signal.SIGTERM)
    os._exit(1)


threading.Thread(target=_crash_process, name="demo-crash", daemon=True).start()

_provider = MeterProvider(
    resource=Resource.create({"service.name": os.environ["OTEL_SERVICE_NAME"]}),
    metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())],
)
metrics.set_meter_provider(_provider)
atexit.register(_provider.shutdown)

_meter = metrics.get_meter("aiops.demo.app")
page_loads = _meter.create_counter(
    "demo.page.loads",
    description="Times the demo page was served",
)
health_checks = _meter.create_counter(
    "health.checks",
    description="Times the health endpoint was called",
)


def _observe_uptime(_options):
    yield Observation(max(0.0, time.time() - STARTED_AT))


def _cpu_times():
    with open("/proc/stat", encoding="ascii") as handle:
        fields = [int(value) for value in handle.readline().split()[1:]]
    idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
    return sum(fields), idle


def _memory_used_percent():
    meminfo = {}
    with open("/proc/meminfo", encoding="ascii") as handle:
        for line in handle:
            key, raw = line.split(":", 1)
            meminfo[key] = int(raw.split()[0])
    total = meminfo["MemTotal"]
    available = meminfo.get("MemAvailable", meminfo["MemFree"])
    if total <= 0:
        return 0.0
    return 100.0 * (total - available) / total


_cpu_sample = {"total": None, "idle": None, "percent": 0.0}


def _observe_cpu_load(_options):
    try:
        total, idle = _cpu_times()
    except OSError:
        yield Observation(_cpu_sample["percent"])
        return
    previous_total = _cpu_sample["total"]
    previous_idle = _cpu_sample["idle"]
    _cpu_sample["total"] = total
    _cpu_sample["idle"] = idle
    if previous_total is not None:
        delta_total = total - previous_total
        delta_idle = idle - previous_idle
        if delta_total > 0:
            _cpu_sample["percent"] = 100.0 * (delta_total - delta_idle) / delta_total
    yield Observation(_cpu_sample["percent"])


def _observe_memory_load(_options):
    try:
        used_percent = _memory_used_percent()
    except OSError:
        return
    yield Observation(used_percent)


_meter.create_observable_gauge(
    "demo.uptime",
    callbacks=[_observe_uptime],
    unit="s",
    description="Seconds since this process started",
)
_meter.create_observable_gauge(
    "demo.cpu.load",
    callbacks=[_observe_cpu_load],
    unit="%",
    description="Host CPU utilization since the previous export",
)
_meter.create_observable_gauge(
    "demo.memory.load",
    callbacks=[_observe_memory_load],
    unit="%",
    description="Host memory in use, as a percentage of MemTotal",
)
FlaskInstrumentor().instrument_app(app)

PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AIOps demo app</title>
  <style>
    :root { color-scheme: light; }
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #f4f6f8;
      color: #1f2933;
      font-family: "Segoe UI", Helvetica, Arial, sans-serif;
    }
    main {
      width: min(32rem, calc(100% - 2rem));
      background: #fff;
      border: 1px solid #e2e8f0;
      border-radius: 12px;
      padding: 2rem;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
    }
    h1 { margin: 0 0 0.25rem; font-size: 1.35rem; }
    .hint { margin: 0 0 1.5rem; color: #52606d; }
    .timer {
      font-variant-numeric: tabular-nums;
      font-size: 3rem;
      font-weight: 650;
      letter-spacing: 0.04em;
      margin: 0;
    }
    footer { margin-top: 1.5rem; font-size: 0.9rem; }
    a { color: #0b5fff; }
  </style>
</head>
<body>
  <main>
    <h1>AIOps demo app</h1>
    <p class="hint">Timer starts when this page loads.</p>
    <p class="timer" id="timer">00:00:00.0</p>
    <footer>Host {{ hostname }} · <a href="/health">health</a></footer>
  </main>
  <script>
    const started = performance.now();
    const pad = (n) => String(n).padStart(2, "0");
    const timer = document.getElementById("timer");

    function tick() {
      const ms = performance.now() - started;
      const total = Math.floor(ms / 100);
      const tenths = total % 10;
      const seconds = Math.floor(total / 10);
      timer.textContent = pad(Math.floor(seconds / 3600)) + ":"
        + pad(Math.floor((seconds % 3600) / 60)) + ":"
        + pad(seconds % 60) + "." + tenths;
      requestAnimationFrame(tick);
    }
    tick();
  </script>
</body>
</html>
"""


def _health_payload():
    uptime = time.time() - STARTED_AT
    running = uptime >= 0
    return {
        "status": "ok" if running else "not ok",
        "detail": "service is running" if running else "service is not running",
        "uptime_seconds": round(uptime, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/")
def workload():
    page_loads.add(1)
    return render_template_string(PAGE, hostname=socket.gethostname())


@app.get("/health")
def health():
    payload = _health_payload()
    code = 200 if payload["status"] == "ok" else 503
    health_checks.add(1, {"status": payload["status"]})
    best = request.accept_mimetypes.best_match(["application/json", "text/html"], default="application/json")
    if best == "text/html" and "text/html" in request.headers.get("Accept", ""):
        color = "#0f7b3a" if code == 200 else "#b42318"
        label = "OK" if code == 200 else "NOT OK"
        page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Health</title>
<style>
  body {{ font-family: "Segoe UI", Helvetica, Arial, sans-serif; display: grid; place-items: center; min-height: 100vh; margin: 0; background: #f4f6f8; }}
  main {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 2rem 3rem; text-align: center; }}
  h1 {{ margin: 0; color: {color}; }}
  p {{ color: #52606d; }}
</style></head>
<body><main><h1>{label}</h1><p>{payload["detail"]}</p></main></body></html>"""
        return page, code
    return jsonify(payload), code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
