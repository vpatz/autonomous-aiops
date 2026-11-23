# Autonomous AIOps demo

Four RHEL VMs on OpenShift Virtualization run a Flask service. A separate load-generator pod calls those services, and a LangGraph workflow compares **what each VM reports** with **what its clients experience**. When it detects an incident, it investigates the affected VM, selects an approved recovery action where available, and checks fresh telemetry to see whether the action worked.

## Try it offline

For a presenter walkthrough with concurrent faults, MLflow traces, and recovery verification, see the [demo sequence](demo.md).

From the repository root, run:

```sh
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r 3_langgraph_workflow/requirements.txt
python3 3_langgraph_workflow/workflow.py --demo
python3 3_langgraph_workflow/workflow.py --self-check
```

`--demo` walks through a killed service, from telemetry silence to verified recovery, using synthetic telemetry and mock MCP tools. It needs no cluster, model endpoint, or Ansible installation. For a local run with **real** Flask telemetry, see the [workflow guide](3_langgraph_workflow/README.md#local-end-to-end-with-real-telemetry).

## How the live demo works

Each call to `/` multiplies two random 75×75 matrices and simulates a 100 ms
I/O wait so concurrent requests overlap. Set `PAGE_IO_DELAY_MS=0` in the service
environment to disable the wait. `/health` skips both the multiplication and the delay.

```text
Load-generator pod ── GET / or /health ──► Flask service on a randomly selected VM
        │                                         │
        │ HTTP outcomes per VM per second         │ guest metrics, logs and request spans
        └─────────── OTLP/HTTP ───────────────────┴──► aiops-workflow:4318
                                                        collector + dashboard
                                                        detector → RCA → selector → executor
                                                           ▲                            │
                                                           └── post-action telemetry ───┘
```

- **Traffic and telemetry.** Across all four VMs, the generator targets 1 request/s, adds 5 each second up to 500/s, holds for 5 seconds, then repeats. Every two seconds it sends HTTP status counts for each VM and second. `no_response` means the client got no HTTP status; an empty count means no completions. These reports never refresh the guest heartbeat. The VM itself exports metrics every two seconds, including host CPU, memory, process uptime, and `service.requests.active` (requests in flight at collection time, not requests/s).
- **Detection.** Rules catch known signals such as telemetry silence, failed spans, and resource pressure. When configured, the anomaly detector also sends the LLM the last **10 seconds** of OTLP data received for that VM, including the generator’s client observations. The model can flag an additional incident by citing evidence. If the model is unavailable or its response is invalid, the rules still apply.
- **Investigation and recovery.** The root-cause agent uses Linux MCP tools to check evidence such as systemd status and journal logs. With Event-Driven Ansible (EDA) configured, a high-confidence diagnosis can trigger one of the two recovery actions below. The stock deployment without EDA uses mock Ansible execution. EDA accepting an event is not proof of recovery: check the controller job and telemetry received after the action.

| Confirmed condition | EDA action | Controller job |
| --- | --- | --- |
| `aiops-demo.service` is down on a running VM | `restart` | [Start the systemd unit over SSH](playbooks/restore-aiops.yml) |
| A previously reporting VM goes silent and KubeVirt confirms it is halted | `restart_vm` | [Start that VM through the KubeVirt API](playbooks/restore-vm.yml) |

Silence alone does not authorize powering on a VM. The [EDA rulebook](rulebooks/restart-aiops.yml) targets one named VM; not every anomaly has an approved recovery action.

## Run it on OpenShift

1. **Prepare the cluster and VMs.** Follow the [cluster setup guide](0_cluster_setup/README.md#3-recreate-credentials-and-deploy-the-demo) to create `my-vm-project`, provision the `linux-mcp-ssh` Secret, and deploy a private copy of `1_vm_services/vm-deploy.yaml` with the matching SSH public key and VM console password. This creates `rhel10-service-1` through `rhel10-service-4` and `linux-mcp`.
2. **Configure integrations.** For model-assisted detection and investigation, configure the optional [`llm-config` Secret](3_langgraph_workflow/README.md#integrating-an-llm). For real recovery, configure the [AAP event stream, controller templates, rulebook activation, and `eda-config` Secret](3_langgraph_workflow/README.md#dispatch-to-in-cluster-event-driven-ansible).

   **When changing clusters:** find the current AAP gateway with `oc -n aap get route aap -o jsonpath='{.spec.host}'`. In that gateway's Automation Decisions UI, copy the new event stream's **full HTTPS POST URL** into `EDA_EVENT_URL` in your private `~/.config/aiops/eda.env`. The stream path is cluster-specific too; replacing only the hostname is not enough. Set the matching Basic-auth credentials, update `eda-config` in `my-vm-project`, and recreate the workflow pod to reload it. In the EDA launcher's AAP credential, set the **controller** URL to `https://<current-gateway-host>/api/controller/`.
3. **Deploy the workflow and traffic generator** from the repo root:

   ```sh
   oc apply -f 3_langgraph_workflow/workflow-pod.yaml
   oc apply -k 5_load_generator
   oc rollout status -n my-vm-project deployment/aiops-load-generator
   ```

   The generator runs in its own pod. It calls each guest through an internal `rhel10-service-N-http` Service on port 5000 and sends its results to `http://aiops-workflow:4318`.

4. **Open the dashboard.** Print the URL and open it in a browser:

   ```sh
   echo "https://$(oc -n my-vm-project get route aiops-workflow -o jsonpath='{.spec.host}')"
   ```

   Each VM card refreshes every two seconds. Watch the CPU, memory, and concurrent-request dials, the recent guest messages, and the anomaly, RCA, and remediation rows. **Latest anomaly detector LLM output** shows the JSON verdict, UTC timestamp, and whether it came from detection or post-action verification. A healthy `{"detected":false}` verdict is distinct from an unavailable model. The dashboard data is also available at `/api/dashboard`.

5. **Exercise a failure.** Once the VM is reporting telemetry, try a targeted scenario:

   ```sh
   python3 4_chaos_injector/chaos_injector.py --scenario kill-service --vm rhel10-service-1
   ```

   See the [chaos guide](4_chaos_injector/README.md) for SSH prerequisites and all eight scenarios, including network outages, blocked or altered telemetry, and host stress. To stop the repeating traffic and remove its HTTP Services, run `oc delete -k 5_load_generator`.

## Repository guide

| Directory | Purpose |
| --- | --- |
| [`0_cluster_setup/`](0_cluster_setup/README.md) | Reference cluster inventory and rebuild instructions |
| [`1_vm_services/`](1_vm_services/) | Flask workload, VM provisioning, and Linux MCP deployment |
| [`2_aiops_heuristic/`](2_aiops_heuristic/) | Standalone predecessor; not required by the live demo |
| [`3_langgraph_workflow/`](3_langgraph_workflow/README.md) | OTLP collector, agents, dashboard, and optional MLflow agent tracing |
| [`4_chaos_injector/`](4_chaos_injector/README.md) | Fault injection CLI |
| [`5_load_generator/`](5_load_generator/README.md) | Standalone generator pod, local CLI, ramp schedule, and response telemetry examples |
| [`rulebooks/`](rulebooks/) and [`playbooks/`](playbooks/) | EDA event routing, controller recovery jobs, and scoped VM-start permissions |
