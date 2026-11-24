# Stage 4: Chaos injector

Injects failure scenarios into the demo VMs to exercise the multi-agent
workflow against unknown failures — randomly, and on one or more VMs
concurrently.

| Scenario | What it does | What the workflow sees |
| --- | --- | --- |
| `kill-service` | `SIGKILL aiops-demo.service` (stays down, `Restart=no`) | `metrics-silent` (persistent) |
| `restart-service` | `systemctl restart aiops-demo.service` | telemetry gap while the unit restarts (the dashboard counts the restart from the uptime reset) |
| `stop-network` | `nmcli networking off` for 60s; recovery is scheduled with a systemd timer **before** the outage | `metrics-silent` while the host is unreachable |
| `restart-vm` | deletes the VMI; the VM controller recreates it (reboot) | `metrics-silent` during the reboot, fresh fault after |
| `shutdown-vm` | sets `spec.running=false` (stays down) | `metrics-silent` (persistent) |
| `block-telemetry` | nftables reject rule on VM egress to port 4318 for 30s | `metrics-silent` while the service keeps running |
| `alter-telemetry` | blocks the real telemetry and publishes fake OTLP metrics (impossible zeros or a CPU spike) for 60s | `zero-telemetry` or `cpu-saturation` |
| `stress-host` | drives **real** host CPU and memory utilization to ~90% for 30s (`--stress-seconds`); a self-terminating python stress unit | `cpu-saturation` + `memory-pressure` |

## Prerequisites

- `oc` with the [oc-virt plugin](https://github.com/kubevirt/kubevirt/releases)
  (`virtctl` installed as `oc-virt`) — in-VM scenarios tunnel SSH through the
  API server with `oc virt ssh`
- a kubeconfig that can patch VMs and exec into VMI subresources in the demo
  namespace
- the linux-mcp SSH key, by default `~/.ssh/mcp_id_ed25519`
  (override with `--identity-file`)
- `opentelemetry-proto` for building the fake telemetry payloads
  (`pip install -r 4_chaos_injector/requirements.txt`)

## Usage

In OpenCode, use `/chaos` with a plain-language description, e.g.
`/chaos high CPU on vm1 for 120 secs` or `/chaos preview block telemetry on vm3 for 45s`.
For `stress-host`, `--stress-resource cpu|memory|both` selects the load; the default is `both`.

```sh
python3 4_chaos_injector/chaos_injector.py --list                 # scenarios
python3 4_chaos_injector/chaos_injector.py --dry-run              # random plan, no action
python3 4_chaos_injector/chaos_injector.py                        # one random scenario, one random VM
python3 4_chaos_injector/chaos_injector.py --random --rounds 3 --targets 2
python3 4_chaos_injector/chaos_injector.py --scenario kill-service --vm rhel10-service-1,rhel10-service-3
python3 4_chaos_injector/chaos_injector.py --scenario all --vm rhel10-service-4   # every scenario once
python3 4_chaos_injector/chaos_injector.py --scenario shutdown-vm --vm rhel10-service-2 --recover-after 300
```

Random selection is reproducible with `--seed N`. `--mixed` picks a different
scenario per VM instead of per round. Timings: `--network-seconds` (60),
`--telemetry-seconds` (30), `--duration` (60, alter-telemetry),
`--recover-after` (0 = stay broken; used to auto-reverse `kill-service` and
`shutdown-vm`).

## Demo notes

- `stop-network` and `block-telemetry` are self-healing: the restore command
  is queued with `systemd-run --on-active=N` **before** the outage, because
  nothing can reach the VM once it is cut off. The telemetry block is a
  dedicated nftables table (`inet chaos`) that the recovery timer deletes
  whole — RHEL 10 has no `iptables` binary.
- With the default in-pod **mock** Ansible MCP server, remediation of
  persistent scenarios (`kill-service`, `shutdown-vm`) is fictional: the
  workflow retries three times and then reports `failed` honestly. Point
  `ANSIBLE_MCP_URL` at a real Ansible MCP server to close the loop for real.
- `alter-telemetry` blocks the VM's real telemetry egress while posting fake
  OTLP metrics to the collector route under the same service name, so the
  collector's latest values are the corrupted ones.
