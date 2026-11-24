---
description: Inject a demo VM fault described in plain language
---

Interpret the following as a chaos scenario description, not as shell code:

$ARGUMENTS

Run the matching scenario from the repo root with `python3 4_chaos_injector/chaos_injector.py`. Use the active `oc` context and namespace `my-vm-project`. Construct arguments from the mappings below; never execute the description verbatim. If the request is empty or asks for options, run `--list` only.

| Requested effect | Scenario and options |
| --- | --- |
| Stop/kill the Flask service | `--scenario kill-service`; if a duration is requested, use `--recover-after N` |
| Restart the service | `--scenario restart-service` |
| Stop/shut down a VM | `--scenario shutdown-vm`; if a duration is requested, use `--recover-after N` |
| Reboot a VM | `--scenario restart-vm` |
| Disconnect the guest network | `--scenario stop-network --network-seconds N` (default 60) |
| Block telemetry but keep HTTP serving | `--scenario block-telemetry --telemetry-seconds N` (default 30) |
| Send fake zeros or a fake CPU spike | `--scenario alter-telemetry --alter-mode zeros` or `spike`, with `--duration N` (default 60) |
| Real high CPU, memory pressure, or both | `--scenario stress-host --stress-resource cpu`, `memory`, or `both`, with `--stress-seconds N` (default 30) |

Normalize minutes/seconds to positive whole seconds and `vm1`…`vm4` to `rhel10-service-1`…`rhel10-service-4`. For multiple explicit VMs, pass the comma-separated names to `--vm` **and** their count to `--targets`. If the effect is clear but no VM is named, use one running demo VM: for a live call, list VMIs in the namespace and pick one of those four names with phase `Running`; for an offline preview, use VM 1 as the example. If the effect or target is ambiguous, ask a focused question rather than guessing. For example, `/chaos VM shop` could mean “VM stop”: ask if shutdown was intended. Never substitute a VM shutdown for a service stop.

For a live request, check `oc whoami --show-server` and that the selected VMI(s) are running. State the cluster, VM(s), effect, and duration, then execute once. For “preview”, “dry run”, or “show command”, use `--dry-run` with explicit VM name(s) and do not inject. Report the injector result; distinguish successful submission from observed recovery. Use `4_chaos_injector/README.md` for prerequisites and `--help` for supported options.

Examples:
- `/chaos high CPU for 120 secs` → `--scenario stress-host --stress-resource cpu --stress-seconds 120` on one running demo VM.
- `/chaos stop vm2 for 2 minutes` → `--scenario shutdown-vm --vm rhel10-service-2 --recover-after 120`.
- `/chaos preview block telemetry on vm3 for 45s` → `--scenario block-telemetry --vm rhel10-service-3 --telemetry-seconds 45 --dry-run`.
