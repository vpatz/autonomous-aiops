# autonomous-aiops

A multi-agent ITOps workflow for Autonomous Remediation from Unknown
Scenarios.

In production deployments running mission critical applications, it is
necessary to predict anomalies, their root causes and apply remediation to
ensure smooth running of IT infrastructure. This repo demonstrates a
multi-agent workflow that remediates error states in a multi-node
distributed system:

- OpenTelemetry sends **metrics, logs and traces** from services running in
  RHEL VMs (OpenShift Virtualization) to a telemetry collector.
- Time windows of that telemetry are examined by the **anomaly detector
  agent**, which triggers the **root cause analysis agent**.
- The RCA agent autonomously iterates to dig deeper through **MCP servers**
  (inner loop): the linux-mcp server runs commands and reads files on the
  affected VM (`systemctl status`, `journalctl`, ...).
- The **remediation selector** chooses a recovery action for the affected VM.
  In the cluster demo, Event-Driven Ansible (EDA) routes the event to a
  controller job. The agent checks fresh telemetry to verify recovery.

The workflow is implemented as a LangGraph `StateGraph`; every iteration of
every agent is kept in an append-only audit trail that drives the demo
dashboard.

```
                         OTLP/HTTP (metrics, logs, traces)
   1_vm_services ─────────────────────────────────────────────► 3_langgraph_workflow
                                                                  collector (:4318)
                                                                  anomaly_detector (agent 1)
                                                                    │ anomaly
                                                                    ▼
                                                        root_cause_analyzer (agent 2)
                                                        inner loop ──► linux-mcp
                                                                    │ root cause
                                                                    ▼
                                                        remediation_selector (agent 3)
                                                                    │ approved plan
                                                                    ▼
                                                        remediation_executor ──► EDA ──► Ansible job
                                                                    │
                                                        outer loop: verify ──► agent 1
```

The two rules in [`rulebooks/restart-aiops.yml`](rulebooks/restart-aiops.yml)
handle different failures:

| Fault | EDA action | Controller job |
| --- | --- | --- |
| `aiops-demo.service` is down on a running VM | `restart` | [Start the unit](playbooks/restore-aiops.yml) on that VM over SSH. |
| A previously reporting VM goes silent and KubeVirt confirms it is halted | `restart_vm` | [Start the VM](playbooks/restore-vm.yml) through the KubeVirt API. |

Both jobs are limited to one named demo VM. Duplicate events leave a running
unit or VM alone. Other chaos scenarios exercise detection and investigation;
not every anomaly has an approved recovery action.

## Quick demo (offline, no cluster needed)

```sh
python3 -m pip install -r 3_langgraph_workflow/requirements.txt
python3 3_langgraph_workflow/workflow.py --demo
python3 3_langgraph_workflow/workflow.py --self-check
```

`--demo` uses synthetic telemetry and mock MCP servers. It prints the full sequence: anomaly
detection, two MCP-backed investigation iterations, the root cause, the
approved remediation plan, its execution, and the verification pass.

See [3_langgraph_workflow/README.md](3_langgraph_workflow/README.md) for
local end-to-end runs with real telemetry, configuration (LLM, MCP
servers), and the deployed mode, and
[4_chaos_injector/README.md](4_chaos_injector/README.md) for injecting
failure scenarios (kill service, stop network, restart/shutdown VM, block or
corrupt telemetry) into the VMs.

## Infra setup (OpenShift Virtualization)

For a fresh cluster, start with the [reference cluster snapshot and rebuild
guide](0_cluster_setup/README.md). It covers platform prerequisites, AAP rulebook
activation and MLflow before the application deployment below.

1. Create the secret the linux-mcp server uses to reach the VMs (private
   keys never live in this repo):
   `ssh-keygen -t ed25519 -f mcp_id_ed25519 -C mcp@rhel`
   `oc create secret generic linux-mcp-ssh -n my-vm-project --from-file=id_ed25519=mcp_id_ed25519`
   Then add the matching public key to `ssh_authorized_keys` and set the VM
   console password in the `rhel9-cloudinit` userdata of
   `1_vm_services/vm-deploy.yaml`.
2. Deploy the VMs and the linux-mcp server:
   `oc apply -f 1_vm_services/vm-deploy.yaml`
   (4 RHEL 10 VMs; each boots, clones this repo and runs
   `1_vm_services/service.py` as `aiops-demo.service`, pushing telemetry to
   `http://aiops-workflow.my-vm-project.svc.cluster.local:4318`)
3. Deploy the multi-agent workflow:
   `oc apply -f 3_langgraph_workflow/workflow-pod.yaml`
   (collector + agents + dashboard on :4318; talks to linux-mcp in-cluster)
4. For real recovery, configure the AAP event stream, rulebook activation,
   controller templates and `eda-config` Secret as described in the
   [workflow setup](3_langgraph_workflow/README.md#dispatch-to-in-cluster-event-driven-ansible).
   `EDA_EVENT_URL` selects EDA **instead of** Ansible MCP execution; without it
   the stock pod uses a mock executor. The optional `llm-config` Secret enables
   model-assisted investigation.
5. The services run cleanly, pushing telemetry only. Inject failures with
   `4_chaos_injector/chaos_injector.py` (kill a service, stop network,
   restart/shutdown a VM, block or corrupt telemetry, or stress the host).
   Watch the agent output and telemetry on the unified
   dashboard at the `aiops-workflow` route.

## Repository layout

| Directory | Contents |
| --- | --- |
| `0_cluster_setup/` | live reference inventory, portable platform manifests and new-cluster runbook |
| `1_vm_services/` | demo service (`service.py`) and VM/linux-mcp deployment (`vm-deploy.yaml`) |
| `2_aiops_heuristic/` | standalone legacy demo: telemetry silence → EDA restart event. Not used or deployed by the LangGraph workflow. |
| `3_langgraph_workflow/` | the multi-agent LangGraph workflow (this demo) |
| `4_chaos_injector/` | chaos scenarios injected into the VMs to exercise the workflow |
| [`5_load_generator/`](5_load_generator/README.md) | separate load-generator pod; `oc apply -k 5_load_generator` to start the repeating HTTP ramp |
| `rulebooks/`, `playbooks/` | EDA event routing, controller recovery jobs and scoped VM-start permissions |
