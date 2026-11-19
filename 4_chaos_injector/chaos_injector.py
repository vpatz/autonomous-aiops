#!/usr/bin/env python3
"""Chaos injector for the autonomous-aiops demo.

Injects failure scenarios randomly into one or more VMs, concurrently:

  1. kill-service      SIGKILL aiops-demo.service (stays down; Restart=no)
  2. restart-service   systemctl restart aiops-demo.service (a fresh
                       dummy-crash fault follows within DUMMY_CRASH_MAX_DELAY)
  3. stop-network      nmcli networking off for --network-seconds (default
                       60); recovery is scheduled with a systemd timer
                       BEFORE the outage so it is self-healing
  4. restart-vm        delete the VMI; the VM controller recreates it (reboot)
  5. shutdown-vm       set spec.running=false (stays down)
  6. block-telemetry   iptables REJECT on VM egress to port 4318 for
                       --telemetry-seconds (default 30); self-healing timer
  7. alter-telemetry   block the real telemetry and publish fake OTLP
                       metrics under the service's name for --duration
                       (default 60): impossible zeros or a CPU spike

In-VM scenarios execute over `oc virt ssh`, tunneled through the API server
(requires the oc-virt plugin and the linux-mcp SSH key, default
~/.ssh/mcp_id_ed25519). VM-level scenarios use `oc`. Fake telemetry is POSTed
to the aiops-workflow collector route.

Verify:

    python3 4_chaos_injector/chaos_injector.py --self-check

Examples:

    python3 4_chaos_injector/chaos_injector.py --list
    python3 4_chaos_injector/chaos_injector.py --dry-run --vm rhel10-service-2
    python3 4_chaos_injector/chaos_injector.py --scenario kill-service --vm rhel10-service-1,rhel10-service-3
    python3 4_chaos_injector/chaos_injector.py --random --rounds 3 --targets 2
    python3 4_chaos_injector/chaos_injector.py --scenario all --vm rhel10-service-4
"""

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

STEP = "chaos injector"
SERVICE_UNIT = "aiops-demo.service"
SSH_PORT = 4318  # the collector port blocked by block-telemetry/alter-telemetry
OTLP_INTERVAL_SECONDS = 5
DEFAULT_IDENTITY = os.path.expanduser("~/.ssh/mcp_id_ed25519")

ZEROS = {"service.uptime": 999.0, "service.cpu.load": 0.0, "service.memory.load": 0.0}
SPIKE = {"service.uptime": 999.0, "service.cpu.load": 97.5, "service.memory.load": 88.0}


class ChaosError(Exception):
    pass


def run_argv(argv, timeout=120):
    """Run a subprocess, returning (returncode, combined output)."""
    try:
        result = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s"
    except FileNotFoundError as exc:
        return 127, str(exc)
    output = (result.stdout + result.stderr).strip()
    return result.returncode, output


class Runner:
    """Executes chaos actions (or records them for --dry-run/self-check)."""

    def __init__(self, cfg, record_only=False):
        self.cfg = cfg
        self.record_only = record_only
        self.recorded = []
        self.timers = []

    def _note(self, kind, detail):
        self.recorded.append({"kind": kind, "detail": detail})

    def ssh(self, vm, command, why, tolerate_failure=False):
        argv = [
            "oc", "virt", "ssh",
            "-i", self.cfg.identity_file,
            "--namespace", self.cfg.namespace,
            "--local-ssh-opts=-o StrictHostKeyChecking=no",
            "--local-ssh-opts=-o UserKnownHostsFile=/dev/null",
            "--local-ssh-opts=-o ConnectTimeout=10",
            "--command", command,
            f"{self.cfg.ssh_user}@vmi/{vm}",
        ]
        detail = f"ssh {vm}: {command} ({why})"
        self._note("ssh", detail)
        if self.record_only:
            print(f"[{STEP}] (dry-run) {detail}", flush=True)
            return 0, "dry-run"
        print(f"[{STEP}] {detail}", flush=True)
        code, output = run_argv(argv)
        if code != 0 and not tolerate_failure:
            # A dropped connection is expected when the command itself cuts
            # networking; anything else is reported but not fatal.
            print(f"[{STEP}] ssh exit {code}: {output[:200]}", flush=True)
        return code, output

    def oc(self, args, why):
        detail = f"oc {' '.join(args)} ({why})"
        self._note("oc", detail)
        if self.record_only:
            print(f"[{STEP}] (dry-run) {detail}", flush=True)
            return 0, "dry-run"
        print(f"[{STEP}] {detail}", flush=True)
        code, output = run_argv(["oc"] + args)
        if code != 0:
            print(f"[{STEP}] oc exit {code}: {output[:200]}", flush=True)
        return code, output

    def otlp(self, vm, values, seconds, why):
        detail = f"otlp {vm}: {json.dumps(values)} every {OTLP_INTERVAL_SECONDS}s for {seconds}s ({why})"
        self._note("otlp", detail)
        if self.record_only:
            print(f"[{STEP}] (dry-run) {detail}", flush=True)
            return
        print(f"[{STEP}] {detail}", flush=True)
        url = self.cfg.collector_url.rstrip("/") + "/v1/metrics"
        deadline = time.monotonic() + max(1, seconds)
        while time.monotonic() < deadline:
            try:
                post_otlp(url, vm, values, insecure=self.cfg.insecure_tls)
            except (OSError, ChaosError) as exc:
                print(f"[{STEP}] otlp post failed: {exc}", flush=True)
            time.sleep(OTLP_INTERVAL_SECONDS)

    def later(self, delay, action, why):
        """Schedule a recovery action after `delay` seconds."""
        detail = f"after {delay}s: {why}"
        self._note("later", detail)
        if self.record_only:
            print(f"[{STEP}] (dry-run) scheduled {detail}", flush=True)
            return
        print(f"[{STEP}] scheduled {detail}", flush=True)
        timer = threading.Timer(delay, action)
        timer.daemon = True
        timer.start()
        self.timers.append(timer)

    def stop(self):
        for timer in self.timers:
            timer.cancel()


# ---------------------------------------------------------------- scenarios

def _unit_tag(prefix, vm):
    return f"{prefix}-{vm}-{int(time.time())}"


def scenario_kill_service(vm, runner, cfg):
    runner.ssh(vm, f"sudo -n systemctl kill --signal=SIGKILL {SERVICE_UNIT}",
               "kill the demo service (stays down; Restart=no)")
    if cfg.recover_after:
        runner.later(
            cfg.recover_after,
            lambda: runner.ssh(vm, f"sudo -n systemctl start {SERVICE_UNIT}",
                               "recovery: restart the killed service"),
            f"restart {SERVICE_UNIT} on {vm}",
        )


def scenario_restart_service(vm, runner, cfg):
    runner.ssh(vm, f"sudo -n systemctl restart {SERVICE_UNIT}",
               "restart the demo service (telemetry gap, then a fresh fault)")


def scenario_stop_network(vm, runner, cfg):
    seconds = cfg.network_seconds
    unit = _unit_tag("chaos-net-restore", vm)
    # Schedule the recovery FIRST: once networking is off, nothing can reach
    # the VM, so the restore must already be queued in systemd.
    runner.ssh(vm, f"sudo -n systemd-run --on-active={seconds} --unit={unit} "
                   f"nmcli networking on",
               f"schedule network recovery in {seconds}s (before the outage)")
    runner.ssh(vm, "sudo -n nmcli networking off",
               f"stop networking for {seconds}s (connection drops; expected)",
               tolerate_failure=True)


def scenario_restart_vm(vm, runner, cfg):
    runner.oc(["delete", "vmi", vm, "-n", cfg.namespace],
              "restart the VM (the controller recreates the VMI: a reboot)")


def scenario_shutdown_vm(vm, runner, cfg):
    runner.oc(["patch", "vm", vm, "-n", cfg.namespace, "--type", "merge",
               "-p", json.dumps({"spec": {"running": False}})],
              "shut the VM down (stays down)")
    if cfg.recover_after:
        runner.later(
            cfg.recover_after,
            lambda: runner.oc(["patch", "vm", vm, "-n", cfg.namespace, "--type", "merge",
                               "-p", json.dumps({"spec": {"running": True}})],
                              "recovery: start the VM back up"),
            f"start VM {vm}",
        )


def _block_telemetry(vm, runner, cfg, seconds):
    unit = _unit_tag("chaos-tel-restore", vm)
    runner.ssh(vm, f"sudo -n systemd-run --on-active={seconds} --unit={unit} "
                   f"iptables -D OUTPUT -p tcp --dport {SSH_PORT} -j REJECT",
               f"schedule telemetry unblock in {seconds}s (before the block)")
    runner.ssh(vm, f"sudo -n iptables -I OUTPUT -p tcp --dport {SSH_PORT} -j REJECT",
               f"block telemetry egress (port {SSH_PORT}) for {seconds}s")


def scenario_block_telemetry(vm, runner, cfg):
    _block_telemetry(vm, runner, cfg, cfg.telemetry_seconds)


def scenario_alter_telemetry(vm, runner, cfg):
    values = ZEROS if cfg.alter_mode == "zeros" else SPIKE
    _block_telemetry(vm, runner, cfg, cfg.duration)
    runner.otlp(vm, values, cfg.duration,
                f"publish fake telemetry ({cfg.alter_mode}) while the real one is blocked")


SCENARIOS = [
    {"name": "kill-service", "run": scenario_kill_service,
     "help": "SIGKILL aiops-demo.service; stays down (Restart=no)"},
    {"name": "restart-service", "run": scenario_restart_service,
     "help": "systemctl restart aiops-demo.service; a fresh fault follows"},
    {"name": "stop-network", "run": scenario_stop_network,
     "help": "nmcli networking off for --network-seconds (default 60), self-healing"},
    {"name": "restart-vm", "run": scenario_restart_vm,
     "help": "delete the VMI; the VM controller recreates it (reboot)"},
    {"name": "shutdown-vm", "run": scenario_shutdown_vm,
     "help": "set spec.running=false; stays down"},
    {"name": "block-telemetry", "run": scenario_block_telemetry,
     "help": "reject VM egress to port 4318 for --telemetry-seconds (default 30)"},
    {"name": "alter-telemetry", "run": scenario_alter_telemetry,
     "help": "block real telemetry and publish fake OTLP values for --duration"},
]
SCENARIOS_BY_NAME = {item["name"]: item for item in SCENARIOS}


# ------------------------------------------------------------- OTLP payload

def build_otlp_metrics(service_name, values):
    """Serialize an OTLP/HTTP metrics export carrying gauges for one service."""
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceRequest,
    )

    request = ExportMetricsServiceRequest()
    resource_metrics = request.resource_metrics.add()
    attribute = resource_metrics.resource.attributes.add()
    attribute.key = "service.name"
    attribute.value.string_value = service_name
    for name, value in values.items():
        metric = resource_metrics.scope_metrics.add().metrics.add()
        metric.name = name
        point = metric.gauge.data_points.add()
        point.as_double = float(value)
    return request.SerializeToString()


def post_otlp(url, service_name, values, insecure=False):
    body = build_otlp_metrics(service_name, values)
    request = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/x-protobuf"}, method="POST",
    )
    context = None
    if insecure:
        import ssl

        context = ssl._create_unverified_context()
    with urllib.request.urlopen(request, timeout=15, context=context) as response:
        if response.status != 200:
            raise ChaosError(f"collector returned HTTP {response.status}")


# ------------------------------------------------------------- cluster glue

def discover_vms(cfg):
    code, output = run_argv(
        ["oc", "get", "vmi", "-n", cfg.namespace, "-o", "name"]
    )
    if code != 0:
        raise ChaosError(f"could not list VMIs: {output[:200]}")
    names = [
        line.strip().split("/", 1)[1]
        for line in output.splitlines()
        if line.strip().startswith("virtualmachineinstance.kubevirt.io/")
    ]
    return names


def discover_collector_url(cfg):
    code, output = run_argv(
        ["oc", "get", "route", "aiops-workflow", "-n", cfg.namespace,
         "-o", "jsonpath={.spec.host}"]
    )
    if code != 0 or not output.strip():
        raise ChaosError(f"could not find the aiops-workflow route: {output[:200]}")
    return f"https://{output.strip()}"


# ------------------------------------------------------------------- rounds

def pick_targets(rng, cfg, available):
    pool = list(cfg.vms) if cfg.vms else available
    if not pool:
        raise ChaosError("no target VMs (pass --vm or run in a namespace with VMIs)")
    count = min(cfg.targets, len(pool))
    return rng.sample(pool, count) if count < len(pool) else list(pool)


def run_round(index, total, scenario_name, targets, cfg):
    print(f"[{STEP}] round {index}/{total}: scenario={scenario_name} "
          f"targets={','.join(targets)}", flush=True)
    results = {}

    def inject(vm):
        runner = Runner(cfg)
        try:
            SCENARIOS_BY_NAME[scenario_name]["run"](vm, runner, cfg)
            return vm, True, ""
        except (ChaosError, OSError) as exc:
            return vm, False, str(exc)

    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        for vm, ok, error in pool.map(inject, targets):
            results[vm] = ok
            status = "ok" if ok else f"FAILED: {error}"
            print(f"[{STEP}] round {index}/{total} vm={vm} -> {status}", flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description="Chaos injector for the AIOps demo")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--list", action="store_true", help="list scenarios and exit")
    parser.add_argument("--scenario", default="random",
                        help="scenario name, 'random' (default), or 'all'")
    parser.add_argument("--vm", action="append", default=[],
                        help="target VM (comma-separated; repeatable). Default: random VMI")
    parser.add_argument("--targets", type=int, default=1,
                        help="VMs to hit per round (sampled from the pool)")
    parser.add_argument("--rounds", type=int, default=1,
                        help="number of injection rounds")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="seconds between rounds")
    parser.add_argument("--mixed", action="store_true",
                        help="pick a different scenario per VM instead of per round")
    parser.add_argument("--network-seconds", type=int, default=60)
    parser.add_argument("--telemetry-seconds", type=int, default=30)
    parser.add_argument("--duration", type=int, default=60,
                        help="alter-telemetry duration")
    parser.add_argument("--alter-mode", choices=["zeros", "spike", "random"], default="random")
    parser.add_argument("--recover-after", type=int, default=0,
                        help="auto-recover persistent scenarios (kill/shutdown) after N seconds")
    parser.add_argument("--seed", type=int, default=None, help="random seed")
    parser.add_argument("--dry-run", action="store_true", help="print actions only")
    parser.add_argument("--namespace", default="my-vm-project")
    parser.add_argument("--ssh-user", default="rheluser")
    parser.add_argument("--identity-file", default=DEFAULT_IDENTITY,
                        help="SSH private key for oc virt ssh")
    parser.add_argument("--collector-url", default="",
                        help="collector base URL (default: the aiops-workflow route)")
    parser.add_argument("--insecure-tls", action="store_true",
                        help="skip TLS verification for the collector route")
    parser.add_argument("--kubeconfig", default="", help="kubeconfig for oc")
    args = parser.parse_args()

    if args.list:
        for item in SCENARIOS:
            print(f"{item['name']:18s} {item['help']}", flush=True)
        return
    if args.self_check:
        self_check()
        return
    if args.kubeconfig:
        os.environ["KUBECONFIG"] = args.kubeconfig

    args.vms = [name for group in args.vm for name in group.split(",") if name.strip()]
    rng = random.Random(args.seed)

    if not args.collector_url:
        args.collector_url = discover_collector_url(args)
    if not args.vms:
        available = discover_vms(args)
        print(f"[{STEP}] discovered VMs: {', '.join(available)}", flush=True)
    else:
        available = args.vms

    if args.dry_run:
        # Render one round of every scenario when the selection is random.
        names = [item["name"] for item in SCENARIOS] if args.scenario == "random" else (
            [args.scenario] if args.scenario != "all"
            else [item["name"] for item in SCENARIOS]
        )
        targets = pick_targets(rng, args, available)
        for index, name in enumerate(names, 1):
            run_round(index, len(names), name, targets, args)
        return

    failures = 0
    for index in range(1, args.rounds + 1):
        if args.scenario == "all":
            names = [item["name"] for item in SCENARIOS]
        elif args.mixed:
            names = [rng.choice(list(SCENARIOS_BY_NAME)) for _ in range(args.targets)]
        else:
            names = [args.scenario if args.scenario != "random"
                     else rng.choice(list(SCENARIOS_BY_NAME))]
        targets = pick_targets(rng, args, available)
        if args.mixed:
            for vm, name in zip(targets, names):
                results = run_round(index, args.rounds, name, [vm], args)
                failures += sum(1 for ok in results.values() if not ok)
        else:
            results = run_round(index, args.rounds, names[0], targets, args)
            failures += sum(1 for ok in results.values() if not ok)
        if index < args.rounds and args.interval > 0:
            time.sleep(args.interval)
    if failures:
        raise SystemExit(f"{STEP}: {failures} injection(s) failed")
    print(f"[{STEP}] done", flush=True)


# ---------------------------------------------------------------- self-check

def self_check():
    if len(SCENARIOS) != 7:
        raise SystemExit(f"{STEP}: expected 7 scenarios, got {len(SCENARIOS)}")
    names = [item["name"] for item in SCENARIOS]
    if len(set(names)) != len(names):
        raise SystemExit(f"{STEP}: duplicate scenario names {names}")

    class Cfg:
        namespace = "my-vm-project"
        ssh_user = "rheluser"
        identity_file = "/dev/null"
        collector_url = "https://collector.example.com"
        insecure_tls = False
        recover_after = 0
        network_seconds = 60
        telemetry_seconds = 30
        duration = 60
        alter_mode = "zeros"

    def record(scenario_name, vm="vm-1", cfg_overrides=None):
        cfg = Cfg()
        for key, value in (cfg_overrides or {}).items():
            setattr(cfg, key, value)
        runner = Runner(cfg, record_only=True)
        SCENARIOS_BY_NAME[scenario_name]["run"](vm, runner, cfg)
        return runner

    kill = record("kill-service")
    ssh_actions = [item for item in kill.recorded if item["kind"] == "ssh"]
    if not ssh_actions or "SIGKILL" not in ssh_actions[0]["detail"]:
        raise SystemExit(f"{STEP}: kill-service did not SIGKILL: {ssh_actions}")
    kill = record("kill-service", cfg_overrides={"recover_after": 120})
    if not any(item["kind"] == "later" for item in kill.recorded):
        raise SystemExit(f"{STEP}: kill-service ignores --recover-after")

    restart = record("restart-service")
    if "systemctl restart" not in restart.recorded[0]["detail"]:
        raise SystemExit(f"{STEP}: restart-service wrong: {restart.recorded}")

    network = record("stop-network")
    ssh_details = [item["detail"] for item in network.recorded if item["kind"] == "ssh"]
    on_index = next(i for i, d in enumerate(ssh_details) if "networking on" in d)
    off_index = next(i for i, d in enumerate(ssh_details) if "networking off" in d)
    if not on_index < off_index:
        raise SystemExit(f"{STEP}: network recovery must be scheduled before the outage: {ssh_details}")
    if "--on-active=60" not in ssh_details[on_index]:
        raise SystemExit(f"{STEP}: network duration not honored: {ssh_details}")

    vm_restart = record("restart-vm")
    oc_detail = vm_restart.recorded[0]["detail"]
    if "delete vmi vm-1" not in oc_detail:
        raise SystemExit(f"{STEP}: restart-vm wrong: {oc_detail}")

    shutdown = record("shutdown-vm")
    oc_detail = shutdown.recorded[0]["detail"]
    if '"running": false' not in oc_detail.replace("False", "false"):
        raise SystemExit(f"{STEP}: shutdown-vm wrong: {oc_detail}")

    block = record("block-telemetry")
    ssh_details = [item["detail"] for item in block.recorded if item["kind"] == "ssh"]
    delete_index = next(i for i, d in enumerate(ssh_details) if "iptables -D" in d)
    insert_index = next(i for i, d in enumerate(ssh_details) if "iptables -I" in d)
    if not delete_index < insert_index:
        raise SystemExit(f"{STEP}: telemetry unblock must be scheduled first: {ssh_details}")
    if "--dport 4318" not in ssh_details[insert_index]:
        raise SystemExit(f"{STEP}: telemetry block must target port 4318: {ssh_details}")

    alter = record("alter-telemetry")
    otlp_actions = [item for item in alter.recorded if item["kind"] == "otlp"]
    if not otlp_actions or "service.cpu.load" not in otlp_actions[0]["detail"]:
        raise SystemExit(f"{STEP}: alter-telemetry did not record fake metrics: {alter.recorded}")
    spike = record("alter-telemetry", cfg_overrides={"alter_mode": "spike"})
    if "97.5" not in spike.recorded[-1]["detail"]:
        raise SystemExit(f"{STEP}: spike mode wrong: {spike.recorded[-1]}")

    # The OTLP payload must parse with the stage-3 collector and be detectable.
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "3_langgraph_workflow"))
    import anomaly_detector
    import collector

    body = build_otlp_metrics("vm-1", ZEROS)
    parsed = collector.parse_metrics(body, received_at=100.0)
    if parsed[0][1].get("service.cpu.load") != 0.0 or parsed[0][0] != "vm-1":
        raise SystemExit(f"{STEP}: OTLP payload roundtrip failed: {parsed}")
    window = {"ever_seen": True, "last_seen_seconds_ago": 1.0,
              "metrics": ZEROS, "metric_events": [], "logs": [], "traces": []}
    if "zero-telemetry" not in anomaly_detector.analyze_window(window)["signals"]:
        raise SystemExit(f"{STEP}: zeros payload must raise the zero-telemetry signal")
    window["metrics"] = SPIKE
    if "cpu-saturation" not in anomaly_detector.analyze_window(window)["signals"]:
        raise SystemExit(f"{STEP}: spike payload must raise cpu-saturation")

    # Seeded selection must be reproducible.
    first, second = random.Random(42), random.Random(42)
    if first.choice(names) != second.choice(names):
        raise SystemExit(f"{STEP}: seeded RNG is not reproducible")
    print(f"{STEP}: self-check ok", flush=True)


if __name__ == "__main__":
    main()
