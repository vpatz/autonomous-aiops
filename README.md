# Autonomous AIOps demo

This demo runs a Flask service on four RHEL VMs in OpenShift Virtualization. A load generator calls each VM. A LangGraph workflow uses OpenTelemetry to compare the VM’s metrics, logs, and traces with the HTTP responses its clients receive.

Rules flag problems such as missing telemetry or CPU or memory usage above 80%. An optional LLM summarizes the incident, and the root-cause agent uses Linux MCP tools to investigate. Event-Driven Ansible (EDA) can start a stopped service, start a confirmed halted VM, or remove a disposable workload causing resource pressure. The workflow checks fresh telemetry for recovery; if the problem has already cleared, it takes no action.

**[Follow the demo walkthrough](demo.md)** to inject faults, watch the per-VM dashboard, and inspect the agent’s decisions in MLflow.

For a terminal-only run without a cluster, see [Offline testing](offline-testing.md).

## Run it on OpenShift

Log in to the target cluster with `oc`. In OpenCode, run [`/prep-cluster`](.opencode/commands/prep-cluster.md) to install the platform prerequisites, then [`/deploy-demo`](.opencode/commands/deploy-demo.md) to deploy the demo and print the **AIOps dashboard** and **MLflow** URLs.

For manual deployment:

1. Follow the [cluster setup guide](0_cluster_setup/README.md) to provision the four VMs, their SSH credentials, and Linux MCP in `my-vm-project`.
2. Configure an [LLM endpoint](3_langgraph_workflow/README.md#integrating-an-llm) for model-assisted investigation and [EDA](3_langgraph_workflow/README.md#dispatch-to-in-cluster-event-driven-ansible) for real recovery. Without EDA, the default executor is a mock. **On a new cluster**, copy the event stream’s full HTTPS POST URL and matching credentials into `eda-config`; changing the hostname alone is not enough.
3. Deploy the workflow and load generator, then open the printed dashboard URL:

   ```sh
   oc apply -f 3_langgraph_workflow/workflow-pod.yaml
   oc apply -k 5_load_generator
   echo "https://$(oc -n my-vm-project get route aiops-workflow -o jsonpath='{.spec.host}')"
   ```

## Inject a fault with OpenCode

Once the dashboard shows **flowing** for the target VM, use [`/chaos`](.opencode/commands/chaos.md) in OpenCode. Run `/chaos` by itself to list scenarios. For example:

```text
/chaos preview block telemetry on vm3 for 45s
/chaos high CPU on vm1 for 120 secs
```

The first command only previews the action. The second runs CPU-only stress on `rhel10-service-1` in the active cluster for up to two minutes. If you omit the VM, the command picks one running demo VM; if the fault is ambiguous, it asks for clarification.

For terminal commands and SSH prerequisites, see the [chaos injector guide](4_chaos_injector/README.md). For configuration and troubleshooting, see the [workflow guide](3_langgraph_workflow/README.md) and [load generator guide](5_load_generator/README.md).
