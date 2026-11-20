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
python3 3_langgraph_workflow/workflow.py --run-once --service local-demo \
  --window-seconds 30 --silence-seconds 6 --verify-wait-seconds 25 \
  --linux-mcp-command  "python3 3_langgraph_workflow/mock_mcp_server.py --role linux" \
  --ansible-mcp-command "python3 3_langgraph_workflow/mock_mcp_server.py --role ansible" &

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 OTEL_SERVICE_NAME=local-demo \
DUMMY_CRASH_MAX_DELAY_SECONDS=10 python3 1_vm_services/service.py
```

The service sends metrics every 5s, then a dummy-crash metric, an ERROR log
and a failed span. The workflow detects it, investigates through the mock
linux-mcp, restarts the service through the mock Ansible MCP, verifies the
post-remediation window, and prints `final status: remediated`.

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

The dashboard at `/` is a unified view built **only from telemetry and
workflow state**: (1) VM status (telemetry freshness, observed-for, signal
counts), (2) service status per VM (process uptime, restarts detected from
uptime resets, host CPU/memory, fault flag), (3) anomalies detected,
(4) RCA output (component, confidence, investigation steps), and (5) the
recommended remediation with its EDA delivery status, plus the agent
findings trail.

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
