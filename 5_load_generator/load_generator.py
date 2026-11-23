#!/usr/bin/env python3
"""Load generator for the autonomous-aiops demo.

Sends traffic to the ``service.py`` instances running as
``aiops-demo.service`` on the KubeVirt VMs. Each request picks a VM and an
endpoint at random, so OTLP spans and counters (``service.page.loads``,
``health.checks``) appear across all services in the dashboard.

VM services listen on :5000 with two endpoints::

    GET /        page with a timer
    GET /health  JSON health payload

VM Services in ``vm-deploy.yaml`` currently expose only port 22, so direct
in-cluster HTTP to ``http://<vm>.my-vm-project.svc:5000`` would need the
Service to expose that port or an ``oc port-forward``. The default mode
therefore drives load with ``oc virt ssh`` (``curl`` inside the guest),
matching ``4_chaos_injector/chaos_injector.py``. A direct-HTTP mode is also
available for local runs or when a route/port-forward is present.

Verify:

    python3 5_load_generator/load_generator.py --self-check
    python3 5_load_generator/load_generator.py --dry-run
    python3 5_load_generator/load_generator.py --dry-run --vm rhel10-service-2
    python3 5_load_generator/load_generator.py --dry-run --url http://localhost:5000 --requests 5

Examples:

    # cluster (via SSH, random VM each request):
    python3 5_load_generator/load_generator.py --requests 100 --rate 5
    python3 5_load_generator/load_generator.py --vm rhel10-service-1,rhel10-service-3 --requests 50 --rate 2

    # direct HTTP (local service.py or port-forward):
    python3 5_load_generator/load_generator.py --url http://localhost:5000 --requests 20 --rate 2
    python3 5_load_generator/load_generator.py --url-template 'http://{vm}.my-vm-project.svc.cluster.local:5000{path}'

    # continuous soak with concurrency:
    python3 5_load_generator/load_generator.py --duration 60 --rate 10 --workers 4
"""

import argparse
import math
import os
import random
import subprocess
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

STEP = "load generator"
DEFAULT_NAMESPACE = "my-vm-project"
DEFAULT_SSH_USER = "rheluser"
DEFAULT_IDENTITY = os.path.expanduser("~/.ssh/mcp_id_ed25519")
RAMP_CYCLE = tuple(range(1, 500, 5)) + (500,) * 10
DEFAULT_VMS = ["rhel10-service-1", "rhel10-service-2", "rhel10-service-3", "rhel10-service-4"]

# Endpoints exposed by 1_vm_services/service.py
ENDPOINTS = [
    ("/", 0.7),        # page load -> service.page.loads
    ("/health", 0.3),  # health check -> health.checks
]


class LoadError(Exception):
    pass


def run_argv(argv, timeout=30):
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s"
    except FileNotFoundError as exc:
        return 127, str(exc)
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output


# ------------------------------------------------------------------ discovery


def discover_vms(cfg):
    """List VMIs in the namespace via oc (same pattern as chaos_injector)."""
    code, output = run_argv(["oc", "get", "vmi", "-n", cfg.namespace, "-o", "name"])
    if code != 0:
        raise LoadError(f"could not list VMIs: {output[:200]}")
    names = [
        line.strip().split("/", 1)[1]
        for line in output.splitlines()
        if line.strip().startswith("virtualmachineinstance.kubevirt.io/")
    ]
    return names


def resolve_targets(cfg, rng):
    """Return the pool of target VMs, respecting explicit --vm and env."""
    if cfg.vms:
        return list(cfg.vms)
    # DASHBOARD_VMS is set in workflow-pod.yaml and mirrors the real inventory.
    env_vms = os.environ.get("DASHBOARD_VMS", "").strip()
    if env_vms:
        return env_vms.split()
    if cfg.dry_run or cfg.urls:
        return list(DEFAULT_VMS)
    # Try cluster discovery; fall back to the four default names so --dry-run
    # and --self-check work without a cluster.
    try:
        discovered = discover_vms(cfg)
        if discovered:
            return discovered
    except LoadError:
        pass
    return list(DEFAULT_VMS)


# ---------------------------------------------------------------- endpoint pick


def pick_endpoint(rng):
    """Weighted random choice among ENDPOINTS (pure function, deterministic with seed)."""
    threshold = rng.random()
    cumulative = 0.0
    for path, weight in ENDPOINTS:
        cumulative += weight
        if threshold < cumulative:
            return path
    return ENDPOINTS[-1][0]


def build_url(vm, path, url_template):
    """Expand a URL template. Supported placeholders: {vm}, {path}."""
    template = url_template.strip()
    # Common shorthand: bare host:port or host without scheme.
    if template and "://" not in template:
        template = "http://" + template
    return template.format(vm=vm, path=path)


# ---------------------------------------------------------------- execution


def http_get(url, timeout):
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode(errors="replace")
            return resp.status, body[:200]
    except urllib.error.HTTPError as exc:
        return exc.code, exc.reason
    except (OSError, ValueError) as exc:
        return None, str(exc)[:200]


def ssh_curl(vm, path, cfg):
    """Run curl inside the VM over oc virt ssh (port 5000 on localhost)."""
    # Use localhost inside the guest; the Service port restriction is irrelevant.
    url = f"http://127.0.0.1:5000{path}"
    # -s silent, -S show errors, -w http_code, -o discard body to stdout is status
    # We want both status and a snippet: curl -s -w "%{http_code}" -o /tmp/body
    cmd = f"curl -sS -m {int(cfg.timeout)} -w ' %{{http_code}}' http://127.0.0.1:5000{path} || true"
    argv = [
        "oc", "virt", "ssh",
        "-i", cfg.identity_file,
        "--namespace", cfg.namespace,
        "--local-ssh-opts=-o StrictHostKeyChecking=no",
        "--local-ssh-opts=-o UserKnownHostsFile=/dev/null",
        "--local-ssh-opts=-o ConnectTimeout=10",
        "--command", cmd,
        f"{cfg.ssh_user}@vmi/{vm}",
    ]
    code, output = run_argv(argv, timeout=cfg.timeout + 15)
    if code != 0:
        return None, f"ssh exit {code}: {output[:200]}"
    # output is "<body> <code>" or error text
    parts = output.rsplit(None, 1)
    if parts and parts[-1].isdigit():
        return int(parts[-1]), parts[0][:200] if len(parts) > 1 else ""
    return None, output[:200]


def do_request(vm, path, cfg):
    """Dispatch one request via the configured transport."""
    if cfg.url_template or cfg.urls:
        if cfg.urls:
            url = cfg.url_by_vm[vm].rstrip("/") + path
        else:
            url = build_url(vm, path, cfg.url_template)
        status, detail = http_get(url, cfg.timeout)
        return {"vm": vm, "path": path, "url": url, "status": status, "detail": detail, "via": "http"}
    # default: SSH curl inside the guest
    status, detail = ssh_curl(vm, path, cfg)
    return {"vm": vm, "path": path, "url": f"ssh://{vm}:5000{path}", "status": status, "detail": detail, "via": "ssh"}


# ---------------------------------------------------------------- main loop


def slots(cfg):
    """Yield nominal (offset in seconds, target RPS) for each request."""
    n = 0
    second = 0
    first = 0
    while cfg.duration is None or second < cfg.duration:
        if cfg.rate is None:
            rate = RAMP_CYCLE[second % len(RAMP_CYCLE)]
            offset = second + (n - first) / rate
        else:
            rate = cfg.rate
            offset = n / rate
        if cfg.duration is not None and offset >= cfg.duration:
            return
        if cfg.duration is None and cfg.requests is not None and n >= cfg.requests:
            return
        yield offset, rate
        n += 1
        if cfg.rate is None and n - first == rate:
            second += 1
            first = n


def run_load(cfg):
    rng = cfg.rng
    if cfg.urls:
        targets = cfg.vms or [urlsplit(url).netloc for url in cfg.urls]
        if len(targets) != len(cfg.urls) or len(set(targets)) != len(targets):
            raise LoadError("supply one distinct --vm name per --url, in matching order")
        if any(not urlsplit(url).hostname or urlsplit(url).username is not None
               or urlsplit(url).scheme not in ("http", "https") for url in cfg.urls):
            raise LoadError("--url requires an http(s) base URL without credentials")
        cfg.url_by_vm = dict(zip(targets, cfg.urls))
    else:
        targets = resolve_targets(cfg, rng)
    if not targets:
        raise LoadError("no target VMs (pass --vm, set DASHBOARD_VMS, or run in a namespace with VMIs)")
    print(f"[{STEP}] targets: {', '.join(targets)}", flush=True)
    print(f"[{STEP}] transport: {'http' if (cfg.url_template or cfg.urls) else 'ssh (oc virt ssh -> curl :5000)'}", flush=True)
    profile = "1/s +5/s each second, hold 500/s for 10s, repeat" if cfg.rate is None else f"{cfg.rate}/s"
    print(f"[{STEP}] target={profile} workers={cfg.workers}", flush=True)

    if cfg.dry_run:
        # Plan only: show what would be hit.
        repeating = cfg.duration is None and cfg.requests is None
        count = sum(RAMP_CYCLE) if repeating else sum(1 for _ in slots(cfg))
        for i in range(1, min(count, 10) + 1):
            vm = rng.choice(targets)
            path = pick_endpoint(rng)
            if cfg.url_template or cfg.urls:
                if cfg.urls:
                    url = cfg.url_by_vm[vm].rstrip("/") + path
                else:
                    url = build_url(vm, path, cfg.url_template)
                print(f"[{STEP}] (dry-run) {i}: GET {url} vm={vm} via=http", flush=True)
            else:
                print(f"[{STEP}] (dry-run) {i}: GET http://127.0.0.1:5000{path} on {vm} via=ssh", flush=True)
        if count > 10:
            print(f"[{STEP}] (dry-run) ... and {count - 10} more", flush=True)
        if repeating:
            print(f"[{STEP}] (dry-run) repeat this {len(RAMP_CYCLE)}s cycle until interrupted")
        return {"ok": 0, "fail": 0, "total": count}

    from response_telemetry import ResponseTelemetry
    collector_url = cfg.collector_url
    if not collector_url:
        if cfg.urls:
            collector_url = "http://localhost:4318"
        else:
            code, host = run_argv(["oc", "get", "route", "aiops-workflow", "-n", cfg.namespace,
                                   "-o", "jsonpath={.spec.host}"])
            if code or not host:
                raise LoadError("cannot discover the workflow route; pass --collector-url BASE_URL")
            collector_url = "https://" + host
    telemetry = ResponseTelemetry(collector_url, targets)
    ok = fail = skipped = 0

    def one_request(n):
        vm = rng.choice(targets)
        path = pick_endpoint(rng)
        try:
            result = do_request(vm, path, cfg)
        except Exception:
            telemetry.record(vm, None)
            raise
        code = result["status"]
        telemetry.record(vm, code)
        success = isinstance(code, int) and 200 <= code < 400
        tag = "ok" if success else "fail"
        print(f"[{STEP}] {n:4d} vm={result['vm']:20s} {result['path']:8s} -> {code} ({tag}) via={result['via']}", flush=True)
        return success

    start = time.monotonic()
    def collect(future):
        nonlocal ok, fail
        try:
            success = future.result()
        except Exception as exc:
            print(f"[{STEP}] request error: {exc}", flush=True)
            success = False
        ok += int(success)
        fail += int(not success)

    with telemetry, ThreadPoolExecutor(max_workers=cfg.workers) as pool:
        pending = set()
        for n, (offset, rate) in enumerate(slots(cfg), 1):
            if cfg.rate is not None and len(pending) >= cfg.workers:
                wait(pending, return_when=FIRST_COMPLETED)
            delay = start + offset - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            done = {f for f in pending if f.done()}
            for f in done:
                collect(f)
            pending -= done
            if cfg.duration is not None and time.monotonic() >= start + cfg.duration:
                break
            if cfg.rate is None:
                # A slow target must not build a queue or cause catch-up bursts.
                if len(pending) >= cfg.workers or time.monotonic() >= start + offset + 1 / rate:
                    skipped += 1
                    continue
            pending.add(pool.submit(one_request, n))
        for f in pending:
            collect(f)

    elapsed = time.monotonic() - start
    print(f"[{STEP}] done: ok={ok} fail={fail} total={ok+fail} skipped={skipped} elapsed={elapsed:.1f}s", flush=True)
    return {"ok": ok, "fail": fail, "total": ok + fail, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Load generator for the AIOps demo VMs")
    parser.add_argument("--self-check", action="store_true", help="run built-in checks")
    parser.add_argument("--dry-run", action="store_true", help="print plan only, no cluster calls")
    parser.add_argument("--vm", action="append", default=[], help="target VM (comma-separated; repeatable)")
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE, help="KubeVirt namespace")
    parser.add_argument("--collector-url", default=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
                        help="OTLP/HTTP collector base URL for two-second response batches")
    parser.add_argument("--url", action="append", default=[], dest="urls", help="direct HTTP base URL (repeatable; e.g. http://localhost:5000). When set, transport is http.")
    parser.add_argument("--url-template", default="", help="URL template with {vm} and {path} placeholders, e.g. http://{vm}.my-vm-project.svc.cluster.local:5000{path}")
    parser.add_argument("--ssh-user", default=DEFAULT_SSH_USER, help="SSH user for oc virt ssh")
    parser.add_argument("--identity-file", default=DEFAULT_IDENTITY, help="SSH private key for oc virt ssh")
    parser.add_argument("--requests", type=int, help="scheduled request slots (ignored with --duration)")
    parser.add_argument("--duration", type=float, default=None, help="run for N seconds instead of a fixed count")
    parser.add_argument("--rate", type=float, help="use fixed requests/second instead of the default 1 to 500 ramp")
    parser.add_argument("--workers", type=int, default=32, help="concurrent workers (default: 32)")
    parser.add_argument("--timeout", type=float, default=5.0, help="per-request timeout seconds")
    parser.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    parser.add_argument("--kubeconfig", default="", help="kubeconfig for oc")
    args = parser.parse_args()

    if args.self_check:
        self_check()
        return
    if args.kubeconfig:
        os.environ["KUBECONFIG"] = args.kubeconfig
    args.vms = [name for group in args.vm for name in group.split(",") if name.strip()]
    # --url may be passed as comma-separated in one flag
    args.urls = [u for group in args.urls for u in group.split(",") if u.strip()]
    if args.urls and args.url_template:
        parser.error("use either --url or --url-template")
    if args.requests is None and args.duration is None and args.rate is not None:
        args.requests = 20
    if args.requests is not None and args.requests < 1:
        raise SystemExit("--requests must be >= 1")
    if args.rate is not None and (not math.isfinite(args.rate) or args.rate <= 0):
        raise SystemExit("--rate must be positive and finite")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")
    if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
        raise SystemExit("--duration must be > 0")
    args.rng = random.Random(args.seed)

    try:
        run_load(args)
    except LoadError as exc:
        raise SystemExit(f"{STEP}: {exc}")
    except KeyboardInterrupt:
        print(f"[{STEP}] interrupted", flush=True)


# ---------------------------------------------------------------- self-check


def self_check():
    # Pure-function checks: no cluster, no network.
    from collections import Counter
    ramp = argparse.Namespace(rate=None, duration=221, requests=None)
    counts = Counter(int(offset) for offset, _ in slots(ramp))
    assert [counts[s] for s in range(221)] == (list(range(1, 500, 5)) + [500] * 10) * 2 + [1]
    rng_a = random.Random(42)
    rng_b = random.Random(42)
    # pick_endpoint must be reproducible with same seed
    picks_a = [pick_endpoint(rng_a) for _ in range(20)]
    picks_b = [pick_endpoint(rng_b) for _ in range(20)]
    if picks_a != picks_b:
        raise SystemExit(f"{STEP}: seeded picks not reproducible: {picks_a} vs {picks_b}")
    if set(picks_a) - {"/", "/health"}:
        raise SystemExit(f"{STEP}: unexpected endpoint in {picks_a}")
    # weighting: / should appear more often than /health over many draws
    rng = random.Random(1)
    counts = {"/": 0, "/health": 0}
    for _ in range(1000):
        counts[pick_endpoint(rng)] += 1
    if counts["/"] <= counts["/health"]:
        raise SystemExit(f"{STEP}: weighting wrong: {counts}")

    # URL template expansion
    if build_url("rhel10-service-1", "/health", "http://{vm}.svc:5000{path}") != "http://rhel10-service-1.svc:5000/health":
        raise SystemExit(f"{STEP}: build_url template failed")
    if build_url("vm-1", "/", "http://localhost:5000{path}") != "http://localhost:5000/":
        raise SystemExit(f"{STEP}: build_url localhost failed")
    # bare host without scheme
    if not build_url("vm-1", "/", "myhost:5000{path}").startswith("http://"):
        raise SystemExit(f"{STEP}: build_url should prepend http://")

    # resolve_targets: explicit --vm wins over env/discovery
    class Cfg:
        namespace = "my-vm-project"
        vms = ["custom-1", "custom-2"]
        urls = []
        url_template = ""
        rng = random.Random(0)
    if resolve_targets(Cfg(), random.Random(0)) != ["custom-1", "custom-2"]:
        raise SystemExit(f"{STEP}: explicit vms not honored")

    # env var DASHBOARD_VMS fallback
    orig = os.environ.get("DASHBOARD_VMS")
    try:
        os.environ["DASHBOARD_VMS"] = "rhel10-service-1 rhel10-service-2"
        class Cfg2:
            namespace = "my-vm-project"
            vms = []
            urls = []
            url_template = ""
            rng = random.Random(0)
        # discover_vms will fail (no oc) -> fallback to DASHBOARD_VMS
        targets = resolve_targets(Cfg2(), random.Random(0))
        if targets != ["rhel10-service-1", "rhel10-service-2"]:
            raise SystemExit(f"{STEP}: DASHBOARD_VMS fallback failed: {targets}")
    finally:
        if orig is None:
            os.environ.pop("DASHBOARD_VMS", None)
        else:
            os.environ["DASHBOARD_VMS"] = orig

    # --vm comma-split parsing (as done in main)
    groups = ["rhel10-service-1,rhel10-service-3", "rhel10-service-2"]
    parsed = [name for group in groups for name in group.split(",") if name.strip()]
    if parsed != ["rhel10-service-1", "rhel10-service-3", "rhel10-service-2"]:
        raise SystemExit(f"{STEP}: vm split failed: {parsed}")

    # dry-run must not call oc / network
    class DryCfg:
        namespace = "my-vm-project"
        vms = ["rhel10-service-1", "rhel10-service-2"]
        urls = []
        url_template = ""
        identity_file = "/dev/null"
        ssh_user = "rheluser"
        rng = random.Random(0)
        requests = 5
        duration = None
        rate = 10
        workers = 1
        timeout = 1
        dry_run = True
    result = run_load(DryCfg())
    if result["total"] != 5:
        raise SystemExit(f"{STEP}: dry-run count wrong: {result}")

    # direct-url dry-run
    class DryHttpCfg(DryCfg):
        vms = ["rhel10-service-1"]
        urls = ["http://localhost:5000"]
        url_template = ""
    result = run_load(DryHttpCfg())
    if result["total"] != 5:
        raise SystemExit(f"{STEP}: http dry-run count wrong: {result}")

    print(f"{STEP}: self-check ok", flush=True)


if __name__ == "__main__":
    main()
