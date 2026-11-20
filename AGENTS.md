# AGENTS.md

The numbered directories are not sequential deployment steps. VM services in
`1_vm_services/` send OTLP directly to `3_langgraph_workflow/`; `4_chaos_injector/`
introduces faults. `2_aiops_heuristic/` is the optional standalone predecessor.
Its entrypoint is still `aiops_agent.py` and its Service is still `aiops-agent`.

## Verify changes

There is no test framework, CI, lint config, or root-level dependency manifest.
Verification is built into the scripts — run from the repo root:

```sh
# stage 3 (multi-agent workflow): everything, including an offline end-to-end run
python3 3_langgraph_workflow/workflow.py --self-check
python3 3_langgraph_workflow/workflow.py --demo          # narrative run, no cluster/LLM/MCP

# stage 4 (chaos injector)
python3 4_chaos_injector/chaos_injector.py --self-check
python3 4_chaos_injector/chaos_injector.py --dry-run     # plan only, no cluster actions

# focused example (workflow --self-check also covers it)
python3 3_langgraph_workflow/remediation_selector.py --self-check

# stage 2 (earlier demo)
python3 2_aiops_heuristic/receive_otel.py --self-check
python3 2_aiops_heuristic/analyze_metrics.py --self-check
python3 2_aiops_heuristic/call_eda.py --dry-run
python3 2_aiops_heuristic/aiops_agent.py --self-check
```

Stage-3 module self-checks need no LLM, MCP servers, or cluster: agents fall back to
deterministic heuristics and in-process/mock MCP servers. Real EDA events (stage 2)
additionally require `EDA_EVENT_URL` and `EDA_EVENT_PASSWORD`.

## Deployment coupling — easy to break

- `3_langgraph_workflow/workflow-pod.yaml` embeds **full copies** of the stage-3 Python
  files in a ConfigMap. Never edit it by hand: regenerate it with
  `python3 3_langgraph_workflow/build_pod_manifest.py` and verify with `--check` (exits
  non-zero on drift) after changing any stage-3 `.py` file.
- `2_aiops_heuristic/agent-pod.yaml` embeds **full copies** of the stage-2 agent Python files
  in a ConfigMap (maintained by hand). Any change to `2_aiops_heuristic/*.py` or
  `2_aiops_heuristic/eda/*.py` must be mirrored into it, or the cluster silently runs stale
  code.
- VMs get `service.py` via `git clone --depth 1 https://github.com/vpatz/autonomous-aiops.git`
  on each `aiops-demo.service` start (`start-service.sh` in `vm-deploy.yaml`).
  Restarting the unit picks up pushed service code; cloud-init changes need separate
  provisioning.
- Both pods pip-install dependencies at startup (`langgraph`, `opentelemetry-proto` for
  stage 3; `opentelemetry-proto` for stage 2); nothing is image-built.

## Dependencies (no requirements file at the root)

- Stage 3 has `3_langgraph_workflow/requirements.txt`; the pod install pins live in
  `build_pod_manifest.py`. Agent instrumentation (`agent_tracing.py`) uses
  `mlflow-tracing` and Kubernetes namespaced auth. `MLFLOW_TRACKING_URI`
  enables it; agent spans go to experiment `aiops-workflow`, not the VM collector.
  The LLM client (`llm.py`, OpenAI-compatible
  `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL`) and the MCP client (stdio + streamable HTTP)
  are stdlib-only. A virtualenv is needed locally; nothing is committed.
- Stage 2 agent scripts need only `opentelemetry-proto`; `analyze_metrics.py`,
  `call_eda.py`, and `eda/send_restart_event.py` are stdlib-only.
- `service.py` needs `flask==3.0.3` plus the OpenTelemetry packages pinned in the
  cloud-init section of `vm-deploy.yaml` (the OTLP log exporter lives at
  `opentelemetry.exporter.otlp.proto.http._log_exporter`, with an underscore, in 1.41.1).
  It reads `/proc/stat` and `/proc/meminfo`, so host CPU/memory metrics only work on
  Linux.

## Wiring notes

- Stage 3 entrypoint is `workflow.py` (modes: `--self-check`, `--demo`, `--run-once`,
  default = collector + poller + dashboard on :4318). `graph.build_graph()` wires the
  agents; nodes route with LangGraph `Command(goto=...)`, and `investigation`/`findings`
  are append-only state channels (the audit trail the dashboard shows).
- The unified dashboard (`render_dashboard_shell` + `dashboard_data` in
  `workflow.py`) derives its content **only from telemetry and run state** — it
  never queries MCP servers. It does list KubeVirt **VM objects** through the
  pod's service account (Role: `virtualmachines` get/list) purely to know which
  cards exist and whether a VM is stopped, so a down VM keeps its card (red,
  `vm down`) even after a pod restart; a failed listing degrades to
  telemetry-only. The page polls `/api/dashboard` every 5s and renders per-VM
  cards (two per row, never removed) with rotating SVG dial gauges (host
  CPU/memory — grayed out at zero when telemetry is not flowing), restart counts
  from decreasing `service.uptime` samples, anomaly/RCA/remediation rows with
  progress bars, and a text area of the last 30s of received messages
  (`TelemetryStore.messages`: one line per OTLP export, SDK-internal metrics
  filtered out). A textbox at the bottom shows all agents' output across VMs
  (from the run `findings` channel, newest last). EDA acceptance is not proof of
  recovery: check the controller job and fresh post-action telemetry.
- Stage 4 (`4_chaos_injector/chaos_injector.py`) injects the eight demo scenarios
  (kill/restart service, stop network 60s, restart/shutdown VM, block telemetry 30s,
  fake telemetry, stress host to ~90% CPU/memory for 30s). In-VM commands run over `oc virt ssh` (requires the oc-virt plugin
  and the linux-mcp key at `~/.ssh/mcp_id_ed25519`); VM ops use `oc delete vmi` /
  `oc patch vm`; fake telemetry is OTLP-posted to the collector route. Timed scenarios
  schedule their own recovery with `systemd-run --on-active=N` before the outage —
  keep that ordering if you touch them. `alter-telemetry` zeros are only detectable
  because the stage-3 detector has the `zero-telemetry` signal — redeploy the workflow
  pod when the detector changes.
- MCP servers are resolved from `LINUX_MCP_URL`/`LINUX_MCP_COMMAND` and
  `ANSIBLE_MCP_URL`/`ANSIBLE_MCP_COMMAND`. In-cluster default for linux-mcp:
  `http://linux-mcp.my-vm-project.svc.cluster.local:8000/mcp` (deployed by
  `vm-deploy.yaml`). `mock_mcp_server.py --role linux|ansible` stands in for both roles
  offline (stdio JSON-RPC, same protocol as the real servers).
- LLM integration: `LLM_BASE_URL` (OpenAI-compatible, include `/v1`), `LLM_MODEL`,
  `LLM_API_KEY`. The pod reads them from an **optional** `llm-config` Secret
  (`secretKeyRef` in workflow-pod.yaml — never commit keys); a missing secret just
  means heuristic fallback. Test an endpoint with `llm.py --probe`. Agents best-effort:
  detector keeps deterministic triggers, the RCA inner loop asks the model for
  per-iteration tool decisions over the live tools/list, the selector's plan is
  validated against approved tools only.
- Stage 2: `receive_otel.py` and `call_eda.py` insert `eda/` into `sys.path`
  (`__file__`-relative) and import `receive_metrics` / `send_restart_event` as top-level
  modules — do not move or rename files without updating those inserts.
- `service.py` runs cleanly and only pushes telemetry (metrics every 5s, Flask
  request spans, an OTLP log pipeline). Faults are injected exclusively by the
  stage-4 chaos injector; the systemd unit has `Restart=no`, so a killed service
  stays down until the workflow remediates it. The detector still recognizes a
  `dummy-crash` counter increment as a fault signal for services that report one.
- Stage 3 verification semantics: a service is "down" when its latest metrics are older
  than `--silence-seconds` (default 10s; services never seen are never restarted); the
  RCA inner loop is bounded by `--max-iterations` (5), the remediation outer loop by
  `--max-attempts` (3). Post-remediation windows are filtered by `since` so only
  telemetry received after the fix counts.
- Local end-to-end test of the full chain (real service.py telemetry + mock MCP servers
  over stdio): see "Local end-to-end with real telemetry" in
  `3_langgraph_workflow/README.md`.
- Local testing without a cluster: `service.py` defaults
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://192.168.64.1:4318` (VM-to-Mac host gateway).

## Cluster facts

- VMs `rhel10-service-1..4`, `linux-mcp` and `aiops-workflow` run in
  `my-vm-project`; the cluster demo uses AAP in `aap`. Stage 2 is not needed.
- Credentials never live in this repo (history was scrubbed once; keep it that way):
  the `linux-mcp-ssh` secret must be created out-of-band (`oc create secret generic
  linux-mcp-ssh -n my-vm-project --from-file=id_ed25519=...`) and the VM console
  password must be set in the `rhel9-cloudinit` userdata before applying
  `vm-deploy.yaml`.
- VM telemetry targets `http://aiops-workflow.my-vm-project.svc.cluster.local:4318`
  (stage 3). To demo stage 2 instead, point the systemd `OTEL_EXPORTER_OTLP_ENDPOINT`
  in `vm-deploy.yaml` back at `aiops-agent` and re-create the VMs.
- `eda-config` supplies the HTTPS event-stream URL and Basic-auth credentials.
  When `EDA_EVENT_URL` is set, the executor dispatches through EDA instead of
  calling Ansible MCP. Without it, the pod defaults to mock Ansible MCP.
- Keep `rulebooks/restart-aiops.yml` and the controller templates in sync with
  the executor: `restart` starts the unit over SSH; `restart_vm` starts a halted
  VM through the API using `playbooks/vm-recovery-rbac.yaml`. Silence alone
  does not authorize powering on a VM. Sync the AAP projects and refresh the
  activation after rulebook edits.
- `2_aiops_heuristic/agent-pod.yaml` sets only `EDA_EVENT_PASSWORD` (value `collect-only`)
  and no `EDA_EVENT_URL`, so stage-2 EDA posting is disabled until a URL is added.
