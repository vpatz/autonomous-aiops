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
DASHBOARD_MESSAGE_SECONDS = 30
SERVICE_ACCOUNT_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
VM_INVENTORY_SECONDS = 15
_inventory = {"at": 0.0, "vms": {}}


def _namespace():
    try:
        with open(os.path.join(SERVICE_ACCOUNT_DIR, "namespace"), encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return os.environ.get("POD_NAMESPACE", "my-vm-project")


def _kube_vm_items(namespace):
    """List KubeVirt VM objects: in-cluster through the service account,
    locally through `oc get -o json`."""
    if os.path.isdir(SERVICE_ACCOUNT_DIR):
        import ssl
        import urllib.request

        with open(os.path.join(SERVICE_ACCOUNT_DIR, "token"), encoding="utf-8") as handle:
            token = handle.read().strip()
        url = (f"https://kubernetes.default.svc/apis/kubevirt.io/v1/"
               f"namespaces/{namespace}/virtualmachines")
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        context = ssl.create_default_context(
            cafile=os.path.join(SERVICE_ACCOUNT_DIR, "ca.crt"))
        with urllib.request.urlopen(request, timeout=5, context=context) as response:
            return json.loads(response.read().decode()).get("items", [])
    import subprocess

    result = subprocess.run(
        ["oc", "get", "vm", "-n", namespace, "-o", "json"],
        capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise OSError(result.stderr.strip()[:200])
    return json.loads(result.stdout).get("items", [])


def vm_inventory(namespace=None, refresh_seconds=VM_INVENTORY_SECONDS):
    """{vm_name: {"running": bool}} for the demo VMs.

    Used only so a VM keeps its dashboard card (red) while it is down: a
    stopped VM sends no telemetry, and the in-memory store forgets it when
    the workflow pod restarts. Dashboard content stays telemetry-derived;
    this is just the VM identity list from the cluster. Errors keep the
    last known inventory (and an empty one degrades to telemetry-only)."""
    now = time.monotonic()
    if _inventory["at"] and now - _inventory["at"] < refresh_seconds:
        return _inventory["vms"]
    try:
        items = _kube_vm_items(namespace or _namespace())
    except (OSError, ValueError):
        return _inventory["vms"]
    vms = {}
    for item in items:
        name = item.get("metadata", {}).get("name", "")
        if not name:
            continue
        spec = item.get("spec", {})
        vms[name] = {"running": spec.get("running", spec.get("runStrategy") != "Halted")}
    _inventory.update({"at": now, "vms": vms})
    return _inventory["vms"]


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
    deliver = remediation_selector.post_eda_event if os.environ.get("EDA_EVENT_URL", "").strip() else None
    ansible = None if deliver else client_for("ANSIBLE", getattr(args, "ansible_mcp_url", None), getattr(args, "ansible_mcp_command", None))

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
        deliver=deliver,
        is_vm_halted=confirmed_halted if deliver else None,
        get_evidence=store.recent_evidence if store is not None else None,
    )
    return app, linux, ansible


def confirmed_halted(name):
    """Check fresh KubeVirt desired power state; an SSH timeout is not evidence."""
    if name not in os.environ.get("DASHBOARD_VMS", "").split():
        return False
    try:
        items = _kube_vm_items(_namespace())
    except (OSError, ValueError, TimeoutError):
        return False
    for item in items:
        if item["metadata"]["name"] == name and not item["metadata"].get("deletionTimestamp"):
            spec = item["spec"]
            return spec.get("running") is False or spec.get("runStrategy") == "Halted"
    return False


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
            # The chaos injector killed the service: telemetry went silent.
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
    print(f"{STEP}: chaos injector killed the service on {service_name}; telemetry went silent", flush=True)
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
        if parsed_url.path == "/api/dashboard":
            payload = dashboard_data(self.store, self.runs, self.runs_lock)
            body = json.dumps(payload, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
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
            page = render_dashboard_shell().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return
        super().do_GET()


def dashboard_data(store, runs, runs_lock, inventory=None):
    """Everything the dashboard renders, as JSON-safe data. Built only from
    telemetry windows and workflow run state — plus the VM inventory, which
    only decides which cards exist and whether a VM is down."""
    with runs_lock:
        runs_snapshot = dict(runs)
    if inventory is None:
        inventory = vm_inventory()

    window_seconds = 600
    services = []
    expected = os.environ.get("DASHBOARD_VMS", "").split()
    for name in sorted(set(expected) | set(store.services()) | set(runs_snapshot) | set(inventory)):
        window = store.window(name, window_seconds)
        metrics = window.get("metrics", {})
        events = window.get("metric_events", [])
        last_seen = window.get("last_seen_seconds_ago")
        vm_state = inventory.get(name)
        if vm_state is not None and not vm_state.get("running", True):
            vm_status = "down"
        elif not window.get("ever_seen"):
            vm_status = "none"
        elif last_seen is not None and last_seen <= state.DEFAULT_SILENCE_SECONDS:
            vm_status = "flowing"
        else:
            vm_status = "silent"
        run = runs_snapshot.get(name)
        run_view = None
        if run and run.get("running"):
            run_view = {"status": "running"}
        elif run:
            run_view = {
                "status": run.get("status"),
                "anomaly": run.get("anomaly", {}),
                "root_cause": run.get("root_cause", {}),
                "plan": run.get("remediation", {}).get("plan", []),
                "result": run.get("remediation_result", {}),
                "attempts": run.get("attempts", 0),
                "max_attempts": run.get("max_attempts", state.DEFAULT_MAX_ATTEMPTS),
                "investigation_steps": len(run.get("investigation", [])),
            }
        services.append({
            "name": name,
            "vm": {
                "status": vm_status,
                "last_seen_seconds_ago": last_seen,
                "observed_for_seconds": window.get("observed_for_seconds"),
                "metrics": len(metrics),
                "logs": len(window.get("logs", [])),
                "traces": len(window.get("traces", [])),
            },
            "messages": store.messages(name, DASHBOARD_MESSAGE_SECONDS),
            "service": {
                "uptime": metrics.get("service.uptime"),
                "restarts": sum(1 for item in events if item.get("metric") == "service.uptime"),
                "cpu": metrics.get("service.cpu.load"),
                "memory": metrics.get("service.memory.load"),
                "active_requests": metrics.get("service.requests.active"),
                "fault": metrics.get("dummy-crash", 0),
            },
            "run": run_view,
        })

    findings = []
    for name in sorted(runs_snapshot):
        run = runs_snapshot[name]
        if not run or run.get("running"):
            continue
        for line in run.get("findings", [])[-30:]:
            findings.append({"service": name, "finding": line})

    return {
        "services": services,
        "findings": findings,
        "meta": {
            "llm": llm.available(),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    }


def render_dashboard_shell():
    """The dashboard page: a static shell that polls /api/dashboard and
    renders cards with rotating dial gauges and progress bars in place, so
    needles sweep smoothly between updates instead of reloading."""
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>AIOps unified view</title>
  <style>
    :root {
      --bg: #0b1220; --card: #141d30; --edge: #293548; --ink: #e6edf6;
      --muted: #8ea0b5; --ok: #34d399; --bad: #f87171; --warn: #fbbf24; --accent: #38bdf8;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; color: var(--ink);
      background: radial-gradient(1200px 600px at 20% -10%, #16233c 0%, var(--bg) 55%);
      font-family: "Segoe UI", Helvetica, Arial, sans-serif;
    }
    header { padding: 1.4rem 2rem .6rem; display: flex; justify-content: space-between;
             flex-wrap: wrap; gap: .5rem; max-width: 78rem; margin: 0 auto; }
    h1 { margin: 0; font-size: 1.45rem; letter-spacing: .01em; }
    h1 span { color: var(--accent); }
    .stats { color: var(--muted); font-size: .9rem; align-self: baseline; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1.1rem;
            padding: 1rem 2rem 2rem; max-width: 78rem; margin: 0 auto;
            align-items: stretch; }
    @media (max-width: 980px) { .grid { grid-template-columns: minmax(0, 1fr); } }
    .card { background: var(--card); border: 1px solid var(--edge); border-left: 4px solid var(--edge);
            border-radius: 16px; padding: 1rem 1.2rem 1.1rem; box-shadow: 0 14px 34px rgba(0,0,0,.35);
            animation: cardIn .5s cubic-bezier(.2,.7,.3,1) both;
            display: flex; flex-direction: column; }
    .card.vm-flowing { border-left-color: var(--ok); }
    .card.vm-silent { border-left-color: var(--bad); }
    .card.vm-down { border-left-color: var(--bad); }
    .card.vm-none { border-left-color: var(--bad); }
    .dial.offline { filter: grayscale(1); }
    @keyframes cardIn { from { opacity: 0; transform: translateY(12px); }
                        to { opacity: 1; transform: none; } }
    .card-head { display: flex; justify-content: space-between; align-items: center; }
    .card-head h2 { margin: 0; font-size: 1.02rem; font-weight: 600; }
    .pill { padding: .14rem .62rem; border-radius: 999px; font-size: .76rem; white-space: nowrap;
            display: inline-flex; align-items: center; gap: .38rem; }
    .pill.ok { background: rgba(52,211,153,.14); color: var(--ok); }
    .pill.bad { background: rgba(248,113,113,.14); color: var(--bad); }
    .pill.warn { background: rgba(251,191,36,.14); color: var(--warn); }
    .pill.muted { background: rgba(142,160,181,.14); color: var(--muted); }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 2.2s ease-in-out infinite; }
    @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .3; } }
    .dials { display: flex; flex-wrap: wrap; gap: 1.2rem; align-items: flex-end; margin: .8rem 0 .2rem; }
    .dial { text-align: center; }
    .dial svg { overflow: visible; }
    .needle { transition: transform 1s cubic-bezier(.35,0,.25,1); }
    .arc { transition: stroke-dasharray 1s ease, stroke .6s ease; }
    .dial-value { font-size: 1.06rem; font-weight: 650; margin-top: -6px; font-variant-numeric: tabular-nums; }
    .dial-label { font-size: .66rem; letter-spacing: .12em; text-transform: uppercase; color: var(--muted); margin-top: .1rem; }
    .side-stats { flex: 1; display: grid; grid-template-columns: 1fr 1fr; gap: .3rem .9rem; font-size: .84rem; }
    .side-stats .k { color: var(--muted); font-size: .64rem; letter-spacing: .1em; text-transform: uppercase; }
    .side-stats b { font-weight: 620; font-variant-numeric: tabular-nums; }
    .bar { height: 8px; border-radius: 6px; background: var(--edge); overflow: hidden; }
    .bar i { display: block; height: 100%; border-radius: 6px; background: var(--accent);
             transition: width 1s ease, background .6s ease; }
    .kv { margin: .55rem 0 .15rem; flex: 1; display: flex; flex-direction: column; }
    .kv .label { color: var(--muted); font-size: .64rem; letter-spacing: .1em; text-transform: uppercase; margin-bottom: .22rem; }
    .sep { border: 0; border-top: 1px dashed var(--edge); margin: .85rem 0 .35rem; }
    .row { display: flex; align-items: center; gap: .55rem; margin: .5rem 0; font-size: .84rem; }
    .row .tag { color: var(--muted); font-size: .64rem; letter-spacing: .1em; text-transform: uppercase; width: 5.2rem; flex: none; }
    .row .text { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .row .pill, .row .chip { max-width: 55%; overflow: hidden; }
    .chip { font-size: .72rem; padding: .1rem .5rem; border-radius: 6px; background: rgba(56,189,248,.12); color: var(--accent); white-space: nowrap; }
    textarea.messages { width: 100%; resize: none; background: #0b1220; color: #9fb3c8;
            border: 1px solid var(--edge); border-radius: 8px; padding: .45rem .6rem;
            margin-top: .25rem; font-family: ui-monospace, Menlo, monospace;
            font-size: .72rem; line-height: 1.35; flex: 1; min-height: 7.2rem; }
    .timeline { padding: .4rem 2rem 3rem; max-width: 78rem; margin: 0 auto; }
    .timeline h2 { font-size: 1rem; color: var(--muted); font-weight: 600; }
    textarea.agent-output { width: 100%; resize: vertical; background: #0b1220; color: #9fb3c8;
            border: 1px solid var(--edge); border-radius: 10px; padding: .6rem .8rem;
            font-family: ui-monospace, Menlo, monospace; font-size: .76rem;
            line-height: 1.4; min-height: 18rem; }
    noscript { color: var(--warn); padding: 2rem; display: block; }
  </style>
</head>
<body>
  <header>
    <h1>AIOps unified view <span>&mdash; autonomous remediation</span></h1>
    <div class="stats" id="stats">loading&hellip;</div>
  </header>
  <main class="grid" id="cards"></main>
  <section class="timeline">
    <h2>Agent output (all VMs, newest last)</h2>
    <textarea id="agentOutput" class="messages agent-output" readonly rows="20"></textarea>
  </section>
  <noscript>This dashboard polls /api/dashboard with JavaScript.</noscript>
  <script>
  (function () {
    'use strict';
    const REFRESH_MS = 5000;
    const cardsRoot = document.getElementById('cards');
    const statsEl = document.getElementById('stats');
    const agentOutput = document.getElementById('agentOutput');
    const cards = new Map();

    function el(tag, cls, text) {
      const node = document.createElement(tag);
      if (cls) node.className = cls;
      if (text != null) node.textContent = text;
      return node;
    }
    function fmtDuration(seconds) {
      if (seconds == null || seconds < 0) return '-';
      seconds = Math.floor(seconds);
      const h = Math.floor(seconds / 3600), m = Math.floor(seconds / 60) % 60, s = seconds % 60;
      if (h) return h + 'h ' + m + 'm';
      if (m) return m + 'm ' + s + 's';
      return s + 's';
    }
    function colorFor(value, warn, bad) {
      if (value == null) return 'var(--muted)';
      if (value >= bad) return 'var(--bad)';
      if (value >= warn) return 'var(--warn)';
      return 'var(--ok)';
    }

    // Semicircle SVG dial with a rotating needle and a value arc.
    const R = 52, CX = 66, CY = 66, ARC_LEN = Math.PI * R;
    function arcPath() {
      return 'M ' + (CX - R) + ' ' + CY + ' A ' + R + ' ' + R + ' 0 0 1 ' + (CX + R) + ' ' + CY;
    }
    function makeDial(title, maximum = 100) {
      const NS = 'http://www.w3.org/2000/svg';
      const wrap = el('div', 'dial');
      const svg = document.createElementNS(NS, 'svg');
      svg.setAttribute('width', '132'); svg.setAttribute('height', '80');
      svg.setAttribute('viewBox', '0 0 132 80');
      for (let i = 0; i <= 10; i++) {
        const a = Math.PI * (1 - i / 10);
        const tick = document.createElementNS(NS, 'line');
        tick.setAttribute('x1', (CX + 43 * Math.cos(a)).toFixed(1));
        tick.setAttribute('y1', (CY - 43 * Math.sin(a)).toFixed(1));
        tick.setAttribute('x2', (CX + 47 * Math.cos(a)).toFixed(1));
        tick.setAttribute('y2', (CY - 47 * Math.sin(a)).toFixed(1));
        tick.setAttribute('stroke', '#3b4a61'); tick.setAttribute('stroke-width', '1.4');
        svg.appendChild(tick);
      }
      const track = document.createElementNS(NS, 'path');
      track.setAttribute('d', arcPath()); track.setAttribute('fill', 'none');
      track.setAttribute('stroke', '#2a3850'); track.setAttribute('stroke-width', '9');
      track.setAttribute('stroke-linecap', 'round');
      svg.appendChild(track);
      const arc = document.createElementNS(NS, 'path');
      arc.setAttribute('d', arcPath()); arc.setAttribute('fill', 'none');
      arc.setAttribute('class', 'arc');
      arc.setAttribute('stroke', 'var(--ok)'); arc.setAttribute('stroke-width', '9');
      arc.setAttribute('stroke-linecap', 'round');
      arc.setAttribute('stroke-dasharray', '0 ' + ARC_LEN.toFixed(1));
      svg.appendChild(arc);
      const needle = document.createElementNS(NS, 'line');
      needle.setAttribute('x1', CX); needle.setAttribute('y1', CY);
      needle.setAttribute('x2', CX); needle.setAttribute('y2', 26);
      needle.setAttribute('stroke', '#e6edf6'); needle.setAttribute('stroke-width', '2.4');
      needle.setAttribute('stroke-linecap', 'round');
      needle.setAttribute('class', 'needle');
      needle.style.transformOrigin = CX + 'px ' + CY + 'px';
      needle.style.transform = 'rotate(-90deg)';
      svg.appendChild(needle);
      const hub = document.createElementNS(NS, 'circle');
      hub.setAttribute('cx', CX); hub.setAttribute('cy', CY); hub.setAttribute('r', '4');
      hub.setAttribute('fill', '#e6edf6');
      svg.appendChild(hub);
      const value = el('div', 'dial-value', '-');
      wrap.appendChild(svg); wrap.appendChild(value);
      wrap.appendChild(el('div', 'dial-label', title));
      return {
        node: wrap,
        set: function (v, unit, muted) {
          // When telemetry is not flowing the gauge is grayed out at zero
          // instead of showing stale values.
          const fraction = muted ? 0 : Math.max(0, Math.min(1, (v || 0) / maximum));
          needle.style.transform = 'rotate(' + (fraction * 180 - 90).toFixed(1) + 'deg)';
          arc.setAttribute('stroke-dasharray', muted ? '0 ' + ARC_LEN.toFixed(1)
            : (fraction * ARC_LEN).toFixed(1) + ' ' + ARC_LEN.toFixed(1));
          arc.setAttribute('stroke', muted ? 'var(--edge)' : (unit === '%' ? colorFor(v, 60, 85) : 'var(--ok)'));
          wrap.style.opacity = muted ? '0.45' : '1';
          wrap.classList.toggle('offline', muted);
          value.textContent = unit === '%' ? (muted ? '0.0%' : v.toFixed(1) + '%')
                                           : (muted ? '—' : String(Math.round(v)));
        },
      };
    }

    function sideStat(parent, key) {
      const cell = el('div');
      cell.appendChild(el('div', 'k', key));
      const val = el('b', null, '-');
      cell.appendChild(val);
      parent.appendChild(cell);
      return val;
    }
    function pill(cls, text, withDot) {
      const p = el('span', 'pill ' + cls, text);
      if (withDot) { p.prepend(el('span', 'dot')); }
      return p;
    }
    function row(parent, tag) {
      const r = el('div', 'row');
      r.appendChild(el('span', 'tag', tag));
      parent.appendChild(r);
      return r;
    }
    function bar(parent) {
      const b = el('div', 'bar');
      const fill = el('i');
      b.appendChild(fill);
      parent.appendChild(b);
      return fill;
    }

    function makeCard(svc) {
      const root = el('div', 'card');
      // Cards appear one after the other: each new card starts its entrance
      // animation a beat later than the ones already on screen.
      root.style.animationDelay = (cards.size * 0.09).toFixed(2) + 's';
      const head = el('div', 'card-head');
      head.appendChild(el('h2', null, svc.name));
      const status = pill('muted', '');
      head.appendChild(status);
      root.appendChild(head);

      const dials = el('div', 'dials');
      const cpu = makeDial('host cpu');
      const mem = makeDial('host memory');
      const requests = makeDial('concurrent requests', 32);
      requests.node.title = 'In-flight HTTP requests at last collection. Dial scale: 0–32; number shows the full count.';
      dials.appendChild(cpu.node); dials.appendChild(mem.node);
      dials.appendChild(requests.node);
      const side = el('div', 'side-stats');
      const uptime = sideStat(side, 'process uptime');
      const restarts = sideStat(side, 'restarts 10m');
      const signals = sideStat(side, 'm / l / t');
      const fault = sideStat(side, 'fault flag');
      dials.appendChild(side);
      root.appendChild(dials);

      root.appendChild(el('hr', 'sep'));

      const anomalyRow = row(root, 'anomaly');
      const anomalyPill = pill('muted', 'no run yet');
      const anomalyText = el('span', 'text', '');
      anomalyRow.appendChild(anomalyPill); anomalyRow.appendChild(anomalyText);

      const rcaRow = row(root, 'rca');
      const rcaWrap = el('div', 'text');
      const rcaText = el('div', 'text', '');
      const rcaFill = bar(rcaWrap);
      rcaRow.appendChild(rcaText); rcaRow.appendChild(rcaWrap);

      const remRow = row(root, 'remedy');
      const remWrap = el('div', 'text');
      const remText = el('div', 'text', '');
      const remFill = bar(remWrap);
      const edaChip = el('span', 'chip', 'EDA: -');
      remRow.appendChild(remText); remRow.appendChild(remWrap); remRow.appendChild(edaChip);

      const msgWrap = el('div', 'kv');
      msgWrap.appendChild(el('div', 'label', 'last 30s messages'));
      const msgBox = document.createElement('textarea');
      msgBox.className = 'messages';
      msgBox.readOnly = true;
      msgBox.setAttribute('rows', '6');
      msgWrap.appendChild(msgBox);
      root.appendChild(msgWrap);

      cardsRoot.appendChild(root);
      return { root: root, status: status, cpu: cpu, mem: mem, requests: requests, uptime: uptime,
               restarts: restarts, signals: signals, fault: fault,
               anomalyPill: anomalyPill, anomalyText: anomalyText,
               rcaText: rcaText, rcaFill: rcaFill,
               remText: remText, remFill: remFill, edaChip: edaChip,
               msgBox: msgBox };
    }

    function setPill(node, cls, text, withDot) {
      node.className = 'pill ' + cls;
      node.textContent = text;
      if (withDot) { node.prepend(el('span', 'dot')); }
    }

    function updateCard(svc, refs) {
      const vm = svc.vm, service = svc.service;
      // VM status is color-coded on the card itself; cards are never
      // removed for a down VM, so the last known state stays visible.
      refs.root.className = 'card vm-' + vm.status;
      if (vm.status === 'flowing') {
        setPill(refs.status, 'ok', 'flowing · ' + Math.round(vm.last_seen_seconds_ago) + 's ago', true);
      } else if (vm.status === 'silent') {
        setPill(refs.status, 'bad', 'silent · ' + Math.round(vm.last_seen_seconds_ago) + 's', true);
      } else if (vm.status === 'down') {
        setPill(refs.status, 'bad', 'vm down', true);
      } else {
        setPill(refs.status, 'bad', 'no telemetry yet', false);
      }
      // Gauges gray out at zero whenever telemetry is not flowing.
      const muted = vm.status !== 'flowing';
      refs.cpu.set(service.cpu, '%', muted || service.cpu == null);
      refs.mem.set(service.memory, '%', muted || service.memory == null);
      refs.requests.set(service.active_requests, '', muted || service.active_requests == null);
      refs.uptime.textContent = fmtDuration(service.uptime);
      refs.restarts.textContent = String(service.restarts);
      refs.signals.textContent = vm.metrics + ' / ' + vm.logs + ' / ' + vm.traces;
      refs.fault.textContent = service.fault > 0 ? String(service.fault) : 'clear';
      refs.fault.style.color = service.fault > 0 ? 'var(--bad)' : 'var(--ok)';

      // Message feed: updated on every poll, regardless of run state.
      const lines = (svc.messages || []).map(function (m) {
        return '[' + Math.round(m.age_seconds) + 's ago] ' + m.text;
      });
      refs.msgBox.value = lines.length ? lines.join('\n')
                                       : 'no messages in the last 30s';
      refs.msgBox.scrollTop = refs.msgBox.scrollHeight;

      const run = svc.run;
      if (!run) {
        setPill(refs.anomalyPill, 'ok', 'not detected', false);
        refs.anomalyText.textContent = '';
        refs.rcaText.textContent = ''; refs.rcaFill.style.width = '0%';
        refs.remText.textContent = ''; refs.remFill.style.width = '0%';
        refs.edaChip.textContent = 'EDA: -';
        return;
      }
      if (run.status === 'running') {
        setPill(refs.anomalyPill, 'warn', 'workflow running', true);
        refs.anomalyText.textContent = '';
        return;
      }
      const anomaly = run.anomaly || {};
      if (anomaly.detected) {
        setPill(refs.anomalyPill, 'bad', (anomaly.signals || []).join(', ') || 'anomaly', false);
        refs.anomalyText.textContent = (anomaly.severity || '') + ' · attempt ' + run.attempts + '/' + run.max_attempts;
      } else {
        setPill(refs.anomalyPill, 'ok', 'not detected', false);
        refs.anomalyText.textContent = run.status || '';
      }
      const rc = run.root_cause || {};
      const confMap = { high: 100, medium: 60, low: 25 };
      const conf = confMap[rc.confidence] || 0;
      refs.rcaText.textContent = (rc.component || '?') + ' — ' + (rc.summary || '');
      refs.rcaFill.style.width = conf + '%';
      refs.rcaFill.style.background = rc.confidence === 'high' ? 'var(--ok)' :
        (rc.confidence === 'medium' ? 'var(--warn)' : 'var(--muted)');
      const result = run.result || {};
      const plan = run.plan || [];
      const step = plan.length ? plan[0].tool + ' ' + ((plan[0].arguments || {}).module || '') : 'none';
      const accepted = result.eda && result.eda.ok;
      refs.remText.textContent = step + ' · ' + (accepted ? 'event accepted' : (result.ok ? 'executed ok' : (result.steps ? 'failed' : 'pending')));
      const attemptPct = Math.min(100, (run.attempts / Math.max(1, run.max_attempts)) * 100);
      refs.remFill.style.width = attemptPct + '%';
      refs.remFill.style.background = result.ok ? 'var(--ok)' : 'var(--warn)';
      const eda = (result.eda || {}).status || 'not configured';
      refs.edaChip.textContent = 'EDA: ' + eda;
    }

    function update(data) {
      const flowing = data.services.filter(s => s.vm.status === 'flowing').length;
      const anomalies = data.services.filter(s => s.run && s.run.anomaly && s.run.anomaly.detected).length;
      statsEl.textContent = data.services.length + ' VMs · ' + flowing + ' flowing · ' +
        anomalies + ' anomaly · llm ' + (data.meta.llm ? 'on' : 'off') +
        ' · updated ' + new Date(data.meta.generated_at).toLocaleTimeString();
      // Cards are created on first sight and never removed: a VM that goes
      // down keeps its card (color-coded red) with the last known state.
      data.services.forEach(function (svc) {
        let refs = cards.get(svc.name);
        if (!refs) { refs = makeCard(svc); cards.set(svc.name, refs); }
        updateCard(svc, refs);
      });
      const reported = new Set(data.services.map(svc => svc.name));
      cards.forEach(function (refs, name) {
        if (reported.has(name)) return;
        refs.root.className = 'card vm-none';
        setPill(refs.status, 'bad', 'no telemetry', false);
        refs.cpu.set(0, '%', true);
        refs.mem.set(0, '%', true);
        refs.requests.set(null, '', true);
        refs.msgBox.value = 'no messages in the last 30s';
      });
      // All agents' output for every VM, newest last, in one textbox.
      agentOutput.value = data.findings.length
        ? data.findings.map(function (item) {
            return '[' + item.service + '] ' + item.finding;
          }).join('\n')
        : 'no agent output yet';
      agentOutput.scrollTop = agentOutput.scrollHeight;
    }

    function poll() {
      fetch('/api/dashboard')
        .then(function (r) { return r.json(); })
        .then(update)
        .catch(function () { statsEl.textContent = 'connection lost — retrying'; });
    }
    setInterval(poll, REFRESH_MS);
    poll();
  })();
  </script>
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
        incident = False
        try:
            telemetry = store.window(service_name, args.window_seconds)
            anomaly = anomaly_detector.detect_window(service_name, telemetry, args.silence_seconds,
                                                      get_evidence=store.recent_evidence)
            if not anomaly["detected"]:
                return
            incident = True
            with WorkflowHandler.runs_lock:
                WorkflowHandler.runs[service_name] = {"running": True}
            app, linux, ansible = build_deps(args, store=store)
            workflow_state = state.initial_state(
                service_name,
                window_seconds=args.window_seconds,
                telemetry=telemetry,
                max_iterations=args.max_iterations,
                max_attempts=args.max_attempts,
            )
            workflow_state["anomaly"] = anomaly
            final = graph.run(app, workflow_state, label=service_name)
            with WorkflowHandler.runs_lock:
                WorkflowHandler.runs[service_name] = final or {"status": "unknown"}
            close_clients(linux, ansible)
        except Exception as exc:  # noqa: BLE001 - a failed run must not kill the poller
            print(f"{STEP}: run for {service_name} failed: {exc}", flush=True)
            with WorkflowHandler.runs_lock:
                WorkflowHandler.runs[service_name] = {"running": False, "status": "error", "error": str(exc)}
        finally:
            if incident:
                finished_at[service_name] = time.monotonic()
            running.discard(service_name)

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
                # Model latency on one VM must not stall checks on other VMs.
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
                quick = anomaly_detector.detect_window(service_name, window, args.silence_seconds,
                                                       get_evidence=store.recent_evidence)
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
            initial = state.initial_state(
                target,
                window_seconds=args.window_seconds,
                telemetry=window,
                max_iterations=args.max_iterations,
                max_attempts=args.max_attempts,
            )
            initial["anomaly"] = quick
            final = graph.run(app, initial, label=target)
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

    # Capture the actual model message across all three OTLP ingestion paths.
    from unittest.mock import patch
    history = collector.TelemetryStore()
    now = time.monotonic()
    for age, vm in [(35, "vm-1"), (20, "vm-1"), (2, "vm-1"), (1, "vm-2")]:
        history.record_metrics(collector.parse_metrics(
            collector.sample_metrics_payload(vm, {"custom.gauge": age}),
            received_at=now-age, retain=history.retain_export))
    body = "full log " * 32
    for _ in range(201):
        history.record_logs(collector.parse_logs(collector.sample_logs_payload("vm-1", body),
                            received_at=now-1, retain=history.retain_export))
    history.record_traces(collector.parse_traces(collector.sample_traces_payload("vm-1"),
                          received_at=now-1, retain=history.retain_export))
    with patch.object(llm, "available", return_value=True), patch.object(
            llm, "chat_json", return_value={"detected": False}) as chat:
        anomaly_detector.detect_window("vm-1", history.window("vm-1", 120), 10,
                                       get_evidence=history.recent_evidence)
        evidence = json.loads(chat.call_args.args[0][1]["content"])["evidence"]
        assert sum(k.startswith("metrics:") for k in evidence) == 2
        assert sum(k.startswith("logs:") for k in evidence) == 201
        assert sum(k.startswith("traces:") for k in evidence) == 1
        assert body in json.dumps(evidence) and "custom.gauge" in json.dumps(evidence)
        assert not any(k.startswith("metrics:") for k in history.recent_evidence("vm-1", since=now-1))

    # The dashboard data carries telemetry + run state; the shell polls it.
    store = collector.TelemetryStore()
    now = time.monotonic()
    store.record_metrics(collector.parse_metrics(
        collector.sample_metrics_payload(
            "vm-1", {"service.uptime": 42.0, "service.cpu.load": 3.0,
                     "service.memory.load": 12.0, "service.requests.active": 7, "dummy-crash": 1}),
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
    data = dashboard_data(store, {"vm-1": run}, threading.Lock(),
                          inventory={"vm-1": {"running": True}, "vm-2": {"running": False}})
    by_name = {item["name"]: item for item in data["services"]}
    service = by_name["vm-1"]
    if service["name"] != "vm-1" or service["vm"]["status"] != "flowing":
        raise SystemExit(f"{STEP}: dashboard data vm view wrong: {service['vm']}")
    if (service["vm"]["metrics"] < 5 or service["service"]["cpu"] != 3.0
            or service["service"]["active_requests"] != 7):
        raise SystemExit(f"{STEP}: dashboard data telemetry wrong: {service}")
    if service["service"]["fault"] != 1:
        raise SystemExit(f"{STEP}: dashboard data fault flag wrong: {service['service']}")
    # A VM the cluster reports as stopped keeps its card, marked down.
    if "vm-2" not in by_name or by_name["vm-2"]["vm"]["status"] != "down":
        raise SystemExit(f"{STEP}: stopped VM must keep a card with status down: {data['services']}")
    if not service["messages"] or not service["messages"][-1]["text"].startswith("metrics:"):
        raise SystemExit(f"{STEP}: dashboard data messages missing: {service.get('messages')}")
    if service["run"]["root_cause"]["component"] != "aiops-demo.service":
        raise SystemExit(f"{STEP}: dashboard data rca wrong: {service['run']}")
    if service["run"]["result"]["eda"]["status"] != "not configured":
        raise SystemExit(f"{STEP}: dashboard data eda wrong: {service['run']}")
    if not data["findings"]:
        raise SystemExit(f"{STEP}: dashboard data findings missing")
    json.dumps(data, default=str)  # must be JSON-serializable
    shell = render_dashboard_shell()
    for marker in ("/api/dashboard", "makeDial", "needle", "setInterval", "poll", "timeline",
                   "repeat(2, minmax(0, 1fr))", "cardIn", "animationDelay", "textarea",
                   "messages", "not detected", "vm-flowing", "vm-silent", "vm-down",
                   "agentOutput", "wrap.style.opacity"):
        if marker not in shell:
            raise SystemExit(f"{STEP}: dashboard shell missing {marker!r}")
    # Cards must never be removed for a down VM, and the status bar is
    # replaced by the card's color code.
    for absent in ("cards.delete", "freshFill", "telemetry freshness"):
        if absent in shell:
            raise SystemExit(f"{STEP}: dashboard shell still contains {absent!r}")
    # The message feed must update before the run-state early returns, or
    # cards without a run never fill the text area.
    if not shell.index("msgBox.value") < shell.index("const run = svc.run"):
        raise SystemExit(f"{STEP}: message feed update must precede the run handling")

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
