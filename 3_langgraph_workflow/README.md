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

The service sends metrics every 5s. Kill it (`kill -9` the python process)
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
set `ANSIBLE_MCP_URL` to a real Ansible MCP server to override it. To also
deliver the recommended remediation to Ansible through an EDA event stream,
set `EDA_EVENT_URL` (plus `EDA_EVENT_USERNAME`/`EDA_EVENT_PASSWORD`); the
executor posts the plan and root cause there and the dashboard shows the
delivery status.

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

- **anomaly detector** — triggering stays deterministic (identical with or
  without a model); the model only phrases the incident summary.
- **root cause analyzer (inner loop)** — each iteration asks the model for
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

The dashboard at `/` is a card-based view built **only from telemetry and
workflow state**: cards flow two per row and are never removed when a VM
goes down — the card stays, color-coded by status (green accent while
telemetry flows, red while silent, with the age in the status pill). Each
card has rotating dial gauges for host CPU and memory, process uptime and
restart counts, the workflow rows — anomaly badge, RCA summary with a
confidence bar, and the recommended remediation with an attempts bar and
its EDA delivery chip — and a text area showing the last 30 seconds of
messages received from that VM (every OTLP metrics/log/span export becomes
one line in the collector's per-service message feed). The page polls
`/api/dashboard` every 5 seconds and updates the cards in place, so needles
and bars animate smoothly, and a textbox at the bottom shows the output of
all agents across every VM (newest last, auto-scrolled); `/api/dashboard`
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
| `LLM_MODEL` | model name |
| `LINUX_MCP_URL` / `LINUX_MCP_COMMAND` | RCA MCP server (HTTP URL or stdio command) |
| `ANSIBLE_MCP_URL` / `ANSIBLE_MCP_COMMAND` | remediation MCP server |
| `MCP_INSECURE_TLS` | `1` to skip TLS verification on MCP HTTP endpoints |
| `WINDOW_SECONDS` | telemetry window analyzed per run (see also `--window-seconds`) |

CLI flags mirror the tunables: `--window-seconds`, `--silence-seconds`,
`--max-iterations` (inner-loop budget), `--max-attempts` (outer-loop
budget), `--cooldown-seconds`, `--verify-wait-seconds`.

## Agents and safety

- **anomaly_detector** (agent 1): deterministic triggers over the telemetry
  window (silence, ERROR logs, failed spans, dummy-crash counter, CPU/memory
  saturation). The LLM only phrases the summary; triggering is identical
  with or without a model.
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
