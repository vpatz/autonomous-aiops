# Stage 3: LangGraph multi-agent workflow

A multi-agent ITOps workflow for autonomous remediation from unknown
scenarios. OpenTelemetry metrics, logs and traces flow into a collector; a
LangGraph graph of three agents detects the anomaly, iterates through MCP
servers to find the root cause (inner loop), assembles an approved
remediation from the Ansible MCP server, executes it, and verifies the
result (outer loop).

```
                         OTLP/HTTP (metrics, logs, traces)
   1_vm_services ─────────────────────────────────────────────► collector.py (:4318)
                                                                  │ time windows
                                                                  ▼
                                              ┌──────────── anomaly_detector (agent 1)
                                              │                 │ anomaly
                                              │                 ▼
                                              │   ┌── root_cause_analyzer (agent 2)
                                              │   │  inner loop: MCP tool per iteration
                                              │   └──► (linux-mcp: commands, files on the VM)
                                              │                 │ root cause
                                              │                 ▼
                                              │    remediation_selector (agent 3)
                                              │      queries approved commands
                                              │      from the Ansible MCP server
                                              │                 │ approved plan
                                              │                 ▼
                                              │    remediation_executor ──► Ansible MCP
                                              │                 │
                                              └─────────────────┘ outer loop: verify
                                                                post-remediation telemetry
```

## Quick start (offline, nothing but Python needed)

```sh
python3 3_langgraph_workflow/workflow.py --demo         # narrative end-to-end run
python3 3_langgraph_workflow/workflow.py --self-check   # every module, with assertions
```

`--demo` uses in-process mock MCP servers (a fake RHEL VM and a fake
Ansible MCP server with an approved-command allowlist) and synthetic
telemetry. No cluster, LLM, or MCP servers required.

## Local end-to-end with real telemetry

### Agent traces in MLflow

In the cluster, open MLflow → workspace **my-vm-project** → experiment
**aiops-workflow** → **Traces**. Each graph invocation creates an
`aiops.workflow` root tagged `aiops.vm`. Expand it to see the detector,
each RCA iteration, the selector, the executor and any verification/retry
passes, with timings, routing decisions and final status. Spans do not
capture raw logs, tool responses, event URLs or credentials.

The workflow uses the MLflow tracing SDK against the internal HTTPS Service,
the OpenShift service CA and `kubernetes-namespaced` authentication with the
workflow pod's service account. Outside the cluster, tracing is off unless
`MLFLOW_TRACKING_URI` is set. To exercise the same instrumented agent code
without real remediation, run `workflow.py --demo` with a tracking URI and
experiment configured. App telemetry sent by VMs to port 4318 is separate
from these agent execution traces.

### Run the collector and service

Run the real collector/workflow against the real demo service, with the
mock MCP servers standing in for the cluster ones:

```sh
# terminal 1: the workflow (collector + agents), mock MCP servers
python3 3_langgraph_workflow/workflow.py --run-once --service local-demo \
  --window-seconds 30 --silence-seconds 6 --verify-wait-seconds 90 \
  --linux-mcp-command  "python3 3_langgraph_workflow/mock_mcp_server.py --role linux" \
  --ansible-mcp-command "python3 3_langgraph_workflow/mock_mcp_server.py --role ansible"

# terminal 2: the demo service (runs cleanly, telemetry only)
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 OTEL_SERVICE_NAME=local-demo \
python3 1_vm_services/service.py
```

The service sends metrics every 2s. Kill it (`kill -9` the python process)
to simulate the chaos injector's `kill-service` scenario: the workflow
detects the silence, investigates through the mock linux-mcp, runs the
approved remediation through the mock Ansible MCP, and waits for telemetry
to resume. Start the service again in terminal 2 and the run verifies and
prints `final status: remediated`. On the cluster the same loop runs for
real, driven by `4_chaos_injector/chaos_injector.py`.

## Deployed mode (OpenShift)

`workflow.py` with no flags runs the collector, the anomaly poller and an
HTML dashboard (agent steps and iterations) on :4318. Deploy with:

```sh
oc apply -f 3_langgraph_workflow/workflow-pod.yaml
```

The pod talks to the `linux-mcp` server deployed by
`1_vm_services/vm-deploy.yaml` (in-cluster service
`linux-mcp.my-vm-project.svc.cluster.local:8000/mcp`) for root cause
analysis. VMs send telemetry to `aiops-workflow.my-vm-project.svc.cluster.local:4318`
(set in the `aiops-demo.service` unit in `vm-deploy.yaml`).

By default the pod runs the **mock** Ansible MCP server in-process
(`ANSIBLE_MCP_COMMAND`) so the full remediation loop works out of the box;
set `ANSIBLE_MCP_URL` to a real Ansible MCP server to override it.

### Dispatch to in-cluster Event-Driven Ansible

In AAP, select the **Basic-auth event stream** attached to the restart rulebook
activation. Use its full HTTPS POST URL from the AAP gateway, not the EDA
management API or the bare `aap-eda-event-stream` Service. The activation
must match `action: restart`, use `service` to select the inventory host,
and launch the approved job for `aiops-demo.service`.

Put `EDA_EVENT_URL`, `EDA_EVENT_USERNAME` and `EDA_EVENT_PASSWORD` in a private
env file outside the checkout (`KEY=value`, one per line), then run:

```sh
oc create secret generic eda-config -n my-vm-project \
  --from-env-file="$HOME/.config/aiops/eda.env" --dry-run=client -o yaml |
  oc apply --server-side -f -
oc delete pod aiops-workflow -n my-vm-project --ignore-not-found
oc apply -f 3_langgraph_workflow/workflow-pod.yaml
```

With `EDA_EVENT_URL` set, EDA replaces MCP execution on VMs in `DASHBOARD_VMS`.
A high-confidence diagnosis of the stopped demo unit selects service recovery.
The dashboard distinguishes event acceptance
from telemetry verification. Check the activation's event history and the
controller job history to confirm the rulebook ran the job.

The same activation also handles `action: restart_vm`. For silent telemetry the
RCA checks the named demo VM's current KubeVirt spec. Only `running: false`
or `runStrategy: Halted` selects VM recovery. The workflow allows four minutes
for fresh telemetry after dispatch. In this demo an intentional shutdown of
an observed demo VM is therefore treated as a fault while the workflow is active.

The `AIOps - restore stopped VM` controller template uses `playbooks/restore-vm.yml`,
the existing demo inventory, and **Prompt on launch** for the host limit. It
runs locally in the execution pod with an encrypted template credential,
not guest SSH. Apply `playbooks/vm-recovery-rbac.yaml` to authorize only
reading and starting the four named VMs. Populate the controller credential
from the operator-generated `aap/aiops-vm-recovery-token` Secret: inject
`token` as `AIOPS_VM_TOKEN` and `ca.crt` as a temporary PEM file whose path is
`AIOPS_VM_CA_FILE`. To rotate, recreate that token Secret and update the
controller credential. The launcher needs Execute on
this template. Sync both AAP projects and refresh the activation after changing
the rulebook. A delayed or repeated event does not reboot an already running VM.

For `cpu-saturation` or `memory-pressure`, `cleanup_pressure` launches
**AIOps - clean up resource pressure**, using the demo inventory, machine SSH
credential, and **Prompt on launch** for the host limit. The job samples guest CPU,
memory, and cgroup usage for two seconds. It removes at most one active service
accounting for at least 10% of host CPU or RAM while that resource is above 80%.
Eligible services are the injector's VM-specific transient `chaos-stress-…` units
and `rheluser`'s disposable `aiops-disposable-*.service` files located directly
in `~/.config/systemd/user/`. To opt in another user-local service, add its exact
unit name to `/etc/aiops/removable-user-services` on that guest (root-owned
file and directory; no group/world write). The job attributes process CPU and
memory to the owning systemd cgroup and records the unit and PIDs. It never
kills arbitrary PIDs or removes the demo, SSH, or networking service.
User-local units are backed up under
`~/.local/state/aiops-recovery/` before being disabled and removed. To restore
one, copy the backup to the original directory, run `systemctl --user daemon-reload`,
and restore its previous enabled/running state. No eligible contributor under
pressure fails the job; pressure already cleared is a no-op. The job checks
boot ID, critical service PIDs, failed units, SSH and `/health`. The workflow
separately waits for fresh post-dispatch CPU **and** memory readings at or below 80%.

### Integrating an LLM

Every agent uses an OpenAI-compatible endpoint when configured, and falls
back to deterministic heuristics when not — the demo runs either way. Three
environment variables drive it: `LLM_BASE_URL` (including `/v1`, e.g. a
vLLM or EnMaaS endpoint), `LLM_MODEL`, and `LLM_API_KEY` (bearer token).

Locally:

```sh
export LLM_BASE_URL=https://<llm-host>/v1 LLM_MODEL=<model> LLM_API_KEY=<token>
python3 3_langgraph_workflow/llm.py --probe     # one live round trip
python3 3_langgraph_workflow/workflow.py --demo  # LLM-driven agents
```

On the cluster, credentials never go in git: the pod reads them from an
(optional) `llm-config` Secret, so a missing secret simply means heuristic
fallback. `set_secrets_in_cluster.sh` does all of this (it sources a local,
gitignored `set_secrets_in_shell.sh` holding the values, or reads them from
the environment):

```sh
# set_secrets_in_shell.sh (gitignored):
#   export LLM_BASE_URL='https://<llm-host>/v1'
#   export LLM_MODEL='<model>'
#   export LLM_API_KEY='<token>'
./set_secrets_in_cluster.sh     # creates llm-config + recreates the pod
```

Equivalently, manually:

```sh
oc create secret generic llm-config -n my-vm-project \
  --from-literal=LLM_BASE_URL=https://<llm-host>/v1 \
  --from-literal=LLM_MODEL=<model> \
  --from-literal=LLM_API_KEY=<token>
oc delete pod aiops-workflow -n my-vm-project
oc apply -f 3_langgraph_workflow/workflow-pod.yaml
```

What changes with the LLM on:

Every poll checks the rules on each previously seen VM. Agent model calls
share a start-time limit: at least 10 seconds between calls for the same
VM and 5 seconds between starts across VMs.

- **anomaly detector** — rules detect incidents; the model summarizes only
  flagged windows. Each live summary prompt includes the rule facts and
  every OTLP metric, log and trace resource received from that VM in the last
  10 seconds, in receipt order, with timestamps, attributes and all data points
  preserved in protobuf JSON. `client_logs:*` entries in the same evidence map
  are observations **about** that VM from `aiops-load-generator`: each log body
  contains one second of HTTP status counts or `no_response` outcomes. The
  model can use these for context. Client observations never refresh guest liveness.
  Recovery checks further restrict this to exports
  received after the action. There is no record cap or prompt truncation: if
  the window exceeds the model's context limit, detection falls back to rules.
  The rule engine uses its configured workflow window and latest host gauges.
  CPU and memory trigger independently at strictly greater than 80% utilization.
- **root cause analyzer (inner loop)** — RCA receives the detector's exact
  per-VM evidence snapshot, including guest OTLP and load-generator outcomes.
  Evidence keys remain stable even if the collector's rolling window advances
  while RCA checks the guest. After remediation, a new investigation uses the
  new verification snapshot. RCA may end with **No remediation needed** when
  fresh guest telemetry since the incident clears the rules. An unknown cause,
  missing guest metrics, or the model's suggestion alone does not qualify.
  This outcome bypasses selection and execution. Each iteration asks the model for
  the next action as a JSON decision (`call_tool` with arguments, or
  `conclude` with a root cause) over the *live* MCP `tools/list` catalog,
  together with the investigation history. Against the real linux-mcp
  server this is where the model can, for example, pick
  `pcp_performance_summary` to cross-check a reported CPU spike against
  real host load. Bounded by `--max-iterations`; unknown tool names fall
  back to the deterministic playbook.
- **remediation selector** — the model assembles the plan strictly from
  the approved commands the Ansible MCP server advertises; steps naming
  anything else are rejected before execution (the self-check proves it).

The dashboard header and `--self-check` output show whether the LLM is
configured, and the findings trail names the tool the model chose at each
iteration.

Each VM card keeps the last 100 anomaly log entries since workflow startup,
oldest first. Each flagged poll adds a rule observation immediately, including
during investigation or cooldown. The model's response appends a separate
line when available. Both push updates to the browser with their India Standard Time timestamp,
phase, source (LLM or rule fallback), severity, and summary. Healthy polls add no lines.
The detector allows 4,096 completion tokens, including model reasoning. If the
endpoint reports an output-length cutoff before a JSON verdict is available,
it retries the same snapshot once with 8,192 tokens. Other failures go directly
to the rule fallback, with the reason shown on the VM card.

The dashboard at `/` is a card-based view built **only from telemetry and
workflow state** for its content. Cards flow two per row and are never
removed when a VM goes down — the cluster's KubeVirt VM inventory (the
pod's service account can list VMs) is merged in only to decide which
cards exist and whether a VM is stopped, so a down VM keeps its card with
a red accent and a `vm down` badge even after a workflow-pod restart.
When telemetry is not flowing, the host CPU/memory dials gray out at zero
instead of showing stale values; while flowing, the card accent is green
(with the age in the status pill). Each card has process uptime and
restart counts, an anomaly badge, collapsible RCA and remediation output
boxes (diagnosis, tool calls, plan, execution results and EDA delivery),
and a text area showing the last 30 seconds of
messages received from that VM. The page polls `/api/dashboard` every 2
seconds and updates the cards in place. `/api/dashboard` (including cross-VM findings)
and `/runs` are plain JSON for scripting.

The manifest embeds the Python sources in a ConfigMap. After changing any
file in this directory:

```sh
python3 3_langgraph_workflow/build_pod_manifest.py          # re-embed
python3 3_langgraph_workflow/build_pod_manifest.py --check  # verify in CI or pre-commit
```

## Configuration

Environment variables (all optional; heuristics apply without an LLM):

| Variable | Purpose |
| --- | --- |
| `LLM_BASE_URL` | OpenAI-compatible endpoint, including `/v1` (vLLM, EnMaaS, ...) |
| `LLM_API_KEY` | bearer token for the endpoint |
| `LLM_MODEL` | optional override; defaults to `rits/meta-models/Muse-Spark-Open-816B-A42B-NVFP4` when an endpoint is configured |
| `LINUX_MCP_URL` / `LINUX_MCP_COMMAND` | RCA MCP server (HTTP URL or stdio command) |
| `ANSIBLE_MCP_URL` / `ANSIBLE_MCP_COMMAND` | remediation MCP server |
| `MCP_INSECURE_TLS` | `1` to skip TLS verification on MCP HTTP endpoints |
| `WINDOW_SECONDS` | telemetry window analyzed per run (see also `--window-seconds`) |

CLI flags mirror the tunables: `--window-seconds`, `--silence-seconds`,
`--max-iterations` (inner-loop budget, default 25 tool calls), `--max-attempts` (outer-loop
budget), `--cooldown-seconds`, `--verify-wait-seconds`.

## Agents and safety

- **anomaly_detector** (agent 1): deterministic triggers over the telemetry
  window (silence, ERROR logs, failed spans, impossible zero readings, CPU/memory
  load). The model cannot add or clear a trigger. An unavailable model or
  invalid summary falls back to the rule explanation.
- **root_cause_analyzer** (agent 2): the inner loop. With an LLM, each
  iteration chooses the next MCP tool call from the live `tools/list`
  result; without one, a deterministic playbook runs `systemctl status` and
  `journalctl` through the linux-mcp server. Every iteration is appended to
  the `investigation` channel (the audit trail).
- **remediation_selector** (agent 3): assembles steps **only** from the
  tools the Ansible MCP server advertises (the approved-command allowlist).
  Steps naming anything else are rejected before execution.
- **remediation_executor**: runs the approved plan, then routes back to the
  anomaly detector with a `since`-filtered telemetry window, so the workflow
  only reports `remediated` once post-remediation telemetry is healthy.

## Files

| File | Role |
| --- | --- |
| `collector.py` | OTLP/HTTP receiver for metrics, logs, traces; time-window store |
| `state.py` | shared `WorkflowState`, node names, defaults |
| `anomaly_detector.py` | agent 1 |
| `root_cause_analyzer.py` | agent 2 (inner loop) |
| `remediation_selector.py` | agent 3 + executor (outer loop) |
| `graph.py` | LangGraph assembly (`build_graph`, streaming `run`) |
| `workflow.py` | entrypoint: self-check / demo / run-once / server modes |
| `llm.py` | OpenAI-compatible client (stdlib only) |
| `mcp_client.py` | MCP client: stdio, streamable HTTP, in-process fake |
| `mock_mcp_server.py` | mock linux/Ansible MCP servers for offline runs |
| `build_pod_manifest.py` | regenerate/verify `workflow-pod.yaml` |
