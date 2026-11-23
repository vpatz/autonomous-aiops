# Demo: two silent VMs, two different causes

**Time:** about 10–15 minutes on a prepared cluster. **Audience question:** “If two VMs stop sending metrics, should we restart both?” Use the dashboard to show the shared symptom, the load generator to show the difference clients experience, and MLflow to show how RCA investigates before choosing a recovery action.

## 1. Prepare the screens

Run commands from the repository root. Use the [cluster setup](README.md#run-it-on-openshift) and [chaos injector prerequisites](4_chaos_injector/README.md#prerequisites) before presenting. For the full live sequence, configure the LLM, Linux MCP, MLflow tracing, and real EDA recovery. Without EDA, the stock executor is a mock and will not start a killed service.

```sh
NS=my-vm-project
oc -n "$NS" get vmi
oc -n "$NS" get pods
oc -n "$NS" rollout status deployment/aiops-load-generator
echo "https://$(oc -n "$NS" get route aiops-workflow -o jsonpath='{.spec.host}')"
```

Open the dashboard URL, **MLflow → workspace `my-vm-project` → experiment `aiops-workflow` → Traces**, and **AAP → Jobs**. In a second terminal, follow the client:

```sh
oc -n my-vm-project logs -f deployment/aiops-load-generator --tail=10
```

## 2. Establish the baseline

Wait for `rhel10-service-1` and `rhel10-service-2` to show **flowing**, successful client responses, and no investigation in progress. Allow at least 30 seconds for a full detector window. If repeating a previous run, allow for the per-VM incident cooldown (default 120 seconds).

Point out three different measurements:

- The generator ramps from **1 to 500 requests/s across all VMs**, holds for 10 seconds, and repeats. The concurrent-request dial shows the peak overlap of HTTP server spans received from that VM in the last 10 seconds. **In flight at export** shows the instantaneous metric, which can be zero even under load.
- Every two seconds the generator sends per-VM, per-second HTTP outcomes. A `200` is a completed response; `no_response` means no HTTP status came back. Neither is a guest heartbeat.
- **Latest anomaly detector LLM output** is the model's verdict. `{"detected":false}` means it found no *additional* anomaly; deterministic rules can still trigger RCA.

## 3. Inject two faults without telling the agents which is which

Run these commands back to back. The first VM's service stays down until recovered. The second VM schedules its own telemetry unblock after 30 seconds.

```sh
python3 4_chaos_injector/chaos_injector.py --scenario kill-service --vm rhel10-service-1
python3 4_chaos_injector/chaos_injector.py --scenario block-telemetry --vm rhel10-service-2
```

Ask the audience to predict the client results **before** opening the RCA findings:

| Watch | VM 1: service killed | VM 2: telemetry egress blocked |
| --- | --- | --- |
| Guest telemetry | Stops; expect `metrics-silent` after the silence threshold | Same symptom while the block is active |
| Client observations | Requests should fail while the unit is down | Requests should still return `200` if the app is healthy |
| Independent host check | Linux MCP can find a failed or inactive systemd unit | Linux MCP can find a running unit despite the gap |

The agents receive telemetry and tool results, not chaos scenario labels. VM 2 is the unfamiliar combination to discuss: *“The collector lost visibility, but clients still have service.”* The evidence need not identify the exact network rule. Model decisions and timing vary; a short fault may clear before a live check finishes.

## 4. Show the RCA inner loop in MLflow

Open an `aiops.workflow` trace tagged `aiops.vm=rhel10-service-1` from the injection time. Expand the root and select the `root_cause_analyzer` spans in order:

1. **Inspect:** in **Outputs**, show `iteration`, `tools`, and `next`. For example, a `get_service_status` call followed by `next: root_cause_analyzer` means the graph will return to RCA with the result.
2. **Follow up:** the next iteration can fetch `get_service_logs` or another advertised diagnostic. The RCA model gets the detector's same 30-second evidence snapshot plus its investigation history. The default budget is **25 tool calls**, not a required number.
3. **Conclude:** point to `component`, `confidence`, and `next: remediation_selector`. MLflow shows the decision and timing; use the dashboard findings for the tool-result excerpts.

If VM 2 also triggered a run, open its separate trace and compare the timestamps and diagnosis. Overlapping runs on different VMs are independent; two symptoms on the same VM do not create two parallel RCA runs.

## 5. Verify recovery, then reset

For VM 1, look for a high-confidence unit diagnosis, an approved EDA `restart`, and the controller job targeting **only `rhel10-service-1`**. Event acceptance is not recovery. Show the job result, then fresh guest uptime and successful client responses. In MLflow, the later `remediation_executor → anomaly_detector` transition is the **outer verification loop**, distinct from `root_cause_analyzer → root_cause_analyzer`.

VM 2 should resume exporting when its timer fires. Don't present that as agent remediation. If VM 1 remains down after the demonstration, restore it explicitly (adjust the SSH key if needed):

```sh
oc virt ssh --namespace my-vm-project -i ~/.ssh/mcp_id_ed25519 \
  --command 'sudo -n systemctl start aiops-demo.service' rheluser@vmi/rhel10-service-1
```

Finish with both cards flowing. To stop the repeating load, run `oc -n my-vm-project scale deployment/aiops-load-generator --replicas=0`; scale back to `1` for the next session.

## Repeatable fallback

If the live model concludes in one step, use the built-in scenario to demonstrate a multi-step trace without injecting another fault:

```sh
oc -n my-vm-project exec aiops-workflow -- env PYTHONPATH=/tmp/pydeps \
  python3 /opt/workflow/workflow.py --demo
```

This uses the instrumented graph with **mock tools and synthetic telemetry**. Show the two `run_command` tool iterations (unit status, then journal), followed by the RCA conclusion. If the cluster is unavailable, use the [offline quick start](README.md#try-it-offline) to show the same sequence in the terminal.
