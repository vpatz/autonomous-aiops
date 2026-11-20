# Rebuild the OpenShift AIOps demo

This is a **demo-scoped reference snapshot and rebuild guide**, not an etcd or
volume backup. [`cluster-state.json`](cluster-state.json) records what was actually
installed. The YAML files here are **new-cluster inputs**, not raw exports:
they omit generated IDs, certificates, credentials and operator status.
Rebuilding creates new VM disks, AAP history and an empty MLflow experiment store.

### Reference installation (20 November 2026)

| Layer | Observed configuration |
|---|---|
| OpenShift | 4.20.46, `stable-4.20`, single combined control-plane/worker, amd64, ~31.5 allocatable CPUs and 125 GiB allocatable memory. Infrastructure platform `None`. |
| Storage | ODF 4.20.21 with **external Ceph**. Default RBD class `ocs-external-storagecluster-ceph-rbd`; RWX class `ocs-external-storagecluster-cephfs`. |
| Virtualization | OpenShift Virtualization 4.20.35; `rhel10` DataSource ready in `openshift-virtualization-os-images`. Four running VMs, each with a 40 GiB RBD block volume. |
| Workflow | `my-vm-project`: `aiops-workflow` (collector, LangGraph agents and dashboard), `linux-mcp`, four VM SSH Services. No stage-2 collector in the active path. |
| Automation | AAP 2.7 in `aap`. Both AIOps templates present. `AIOps demo service recovery` activation running with two rules on `EDA event stream 1`. |
| Tracing | Open Data Hub 3.6.0, `opendatahub` application namespace; MLflow 3.17.0 available, one worker/replica, SQLite and artifacts on a 10 GiB PVC. Workspace `my-vm-project`, experiment `aiops-workflow`. The overall `default-dsc` reported `NotReady` at capture; the MLflow component itself was ready. |

To refresh the OpenShift inventory with the active `oc` context, run from the
repository root. The script only reads resources and emits selected fields; it
does not read Secrets or export ConfigMaps. AAP application objects listed above
were verified separately in the AAP API.

```sh
oc whoami --show-server
tmp=$(mktemp) && python3 0_cluster_setup/capture.py > "$tmp" &&
  mv "$tmp" 0_cluster_setup/cluster-state.json
```

## 1. Provision the base cluster and storage

Start with a fresh OpenShift 4.20 cluster. For bare metal, follow the
[OpenShift installation guide](https://docs.redhat.com/en/documentation/openshift_container_platform/4.20)
for your platform: supply your own DNS (`api` and wildcard `apps`), networking,
pull secret, installer SSH key and install disks. Those inputs cannot be
reconstructed from a running cluster. Provide an amd64 schedulable node with
hardware virtualization/KVM, cluster-admin `oc` access, registry entitlements,
and egress to GitHub, PyPI, the RHEL/UBI repositories and your LLM endpoint.
Use `virtctl` matching the cluster's KubeVirt release (`oc-virt` plugin for the
chaos injector).

```sh
oc get clusterversion version
oc get nodes -o wide
oc get co
```

Install ODF `stable-4.20` and connect it to **your** external Ceph provider, or
provide equivalent CSI storage. Installing the operator alone does not
create the pools or supply Ceph credentials. Provide a default filesystem PVC
class, snapshot/clone-capable RWO block storage for the VM disks (160 GiB total),
RWO filesystem storage for MLflow, and RWX for AAP Hub (100 GiB in this setup).
If class names differ, adapt private copies of `aap.yaml`, `mlflow.yaml` and
`1_vm_services/vm-deploy.yaml`. Check `oc get sc,volumesnapshotclass`.

## 2. Install operators, then their instances

In **OperatorHub**, install the following packages (also see the installed CSVs
in the snapshot). Use the indicated target namespaces/install scopes. Wait for
each CSV to reach `Succeeded` before creating its CR. OLM resolves the ODF
dependency subscriptions; do not manually install every CSV in the snapshot.

| Package | Catalog / channel | Install namespace |
|---|---|---|
| `openshift-cert-manager-operator` | Red Hat / `stable-v1` | `cert-manager-operator` |
| `kubevirt-hyperconverged` | Red Hat / `stable` | `openshift-cnv` |
| `ansible-automation-platform-operator` | Red Hat / `stable-2.7` | `aap` |
| `opendatahub-operator` | Community / `fast-3` | `openshift-operators` (all namespaces) |

Run these commands from the **repo root** on the **new** cluster:

```sh
oc get csv -A
oc create namespace my-vm-project
oc apply -f 0_cluster_setup/hyperconverged.yaml
oc wait -n openshift-cnv --for=condition=Available hco/kubevirt-hyperconverged --timeout=20m
oc wait -n openshift-virtualization-os-images --for=condition=Ready datasource/rhel10 --timeout=30m
oc apply -f 0_cluster_setup/aap.yaml
oc wait -n aap --for=condition=Successful ansibleautomationplatform/aap --timeout=30m
```

If the RHEL image isn't ready, inspect `dataimportcron/rhel10-image-cron` and
`is/rhel10-guest` in `openshift-virtualization-os-images`. The reference imports
`registry.redhat.io/rhel10/rhel-guest-image` and clones a snapshot; verify registry
access and the storage profile before creating VMs. Complete AAP's first login
and subscription setup on the **new** `aap` Route.

For tracing, let ODH initialize `default-dsci` with applications namespace
`opendatahub`. Check cert-manager and the ODH data-science gateway before
deploying MLflow. On a fresh cluster create the demo DSC below. If there is
already a DSC, enable `dashboard.standard` and `mlflowoperator` on that instance
instead of replacing its other components.

```sh
oc get certmanagers.operator.openshift.io,dsci,gatewayconfigs
oc apply -f 0_cluster_setup/datasciencecluster.yaml
oc wait --for=condition=MLflowOperatorReady dsc/default-dsc --timeout=15m
oc apply -f 0_cluster_setup/mlflow.yaml
oc wait --for=condition=Available mlflows.mlflow.opendatahub.io/mlflow --timeout=15m
oc -n opendatahub get pvc/mlflow-pvc deploy/mlflow
oc get consolelink mlflow
```

## 3. Recreate credentials and deploy the demo

1. Generate a fresh SSH key outside Git (e.g. `ssh-keygen -t ed25519 -f ~/.ssh/mcp_id_ed25519`).
   The demo SSH clients need an unattended key. Create the matching cluster Secret:
   ```sh
   oc -n my-vm-project create secret generic linux-mcp-ssh \
     --from-file=id_ed25519="$HOME/.ssh/mcp_id_ed25519"
   ```
2. Make a **private copy outside the checkout** of `1_vm_services/vm-deploy.yaml`.
   In the `rhel9-cloudinit` userdata replace **all** example authorized keys with
   your own public key(s), and set the VM console password or disable password
   login. Set `LINUX_MCP_BASE_URL` to `https://linux-mcp-my-vm-project.<apps-domain>`
   using `oc get ingress.config.openshift.io cluster -o jsonpath='{.spec.domain}'`.
   Keep the four VM names and namespace unless you also update the workflow
   allowlist, AAP inventory, rulebook, RBAC and DNS. Apply this private file
   with `oc create -f /path/to/private/vm-deploy.yaml` on the new cluster.
3. Prepare `llm-config` and `eda-config` in `my-vm-project` from your own
   private env files via `oc create secret generic NAME --from-env-file=FILE
   -n my-vm-project`. `llm-config`: `LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY`.
   `eda-config`: `EDA_EVENT_URL` (full HTTPS **POST** URL from step 4),
   `EDA_EVENT_USERNAME`, `EDA_EVENT_PASSWORD`. Both Secrets are optional for
   the offline/mock demo. Never copy the source cluster's service-account tokens,
   cloud-init Secret, SSH private key, kubeconfig, or event-stream URL into Git.
4. Deploy `oc apply -f 3_langgraph_workflow/workflow-pod.yaml`. This creates the
   workflow SA, namespaced VM/MLflow RBAC, ConfigMap, Service, Route and Pod.
   It reads the optional Secrets at startup. If you create/change a Secret
   later, delete **only** `pod/aiops-workflow` in `my-vm-project` and reapply
   the manifest. The VMs clone `main` on each service start; forks must update
   the clone URL in the private cloud-init copy.

## 4. Connect real Ansible recovery (AAP application configuration)

These are **AAP objects**, not resources restored by `oc apply`. Use the AAP
UI/API, in organization `Default`. See the [EDA contract](../3_langgraph_workflow/README.md#dispatch-to-in-cluster-event-driven-ansible).

1. Create Git project **AIOps remediation** in both Controller and Automation
   Decisions, pointing to this repo on `main`. Sync both. In Controller create
   **AIOps demo VMs** inventory with group `aiops_vms` and the four hosts
   `rhel10-service-1` through `-4`, each with `ansible_host` set to
   `<name>.my-vm-project.svc.cluster.local`.
2. Create the two templates below with those **exact names**; the rulebook
   references them. Use the demo project/inventory, **Prompt on launch for
   Limit**, and allow the dedicated EDA launcher user to execute only these jobs.

   | Template | Playbook | Credential |
   |---|---|---|
   | `AIOps - restore demo service` | `playbooks/restore-aiops.yml` | Machine credential: `rheluser` and the new VM SSH key; the unit is started via sudo. |
   | `AIOps - restore stopped VM` | `playbooks/restore-vm.yml` | Apply `playbooks/vm-recovery-rbac.yaml`. In an AAP custom credential type inject `token` as `AIOPS_VM_TOKEN` and `ca` as a temporary file (`file.template: "{{ ca }}"`, `AIOPS_VM_CA_FILE: "{{ tower.filename }}"`). Populate it privately from `aap/aiops-vm-recovery-token` (`token`, `ca.crt`). |

3. In Automation Decisions create a **Basic Event Stream** credential and
   event stream (reference name `EDA event stream 1`). Create a **Red Hat
   Ansible Automation Platform** credential for the launcher user with the
   new gateway's `https://<aap-route>/api/controller/` URL and TLS verification.
4. Create activation **AIOps demo service recovery**, use the synced
   `restart-aiops.yml` rulebook and Default Decision Environment. Map source
   `aiops_events` to the new event stream and attach the launcher credential.
   Verify `running` and two rules. Use **this stream's** URL and Basic-auth
   values in `eda-config` (step 3). The workflow then dispatches to EDA
   instead of mock Ansible MCP.

## 5. Verify the new installation

```sh
oc -n my-vm-project get vm,vmi,dv,pod,svc,route
host=$(oc -n my-vm-project get route aiops-workflow -o jsonpath='{.spec.host}')
curl -fsS "https://$host/api/dashboard"
oc -n my-vm-project logs aiops-workflow --tail=20
```

Check that all four cards report **flowing** (fresh VM telemetry, not just a
Ready Pod). For an MLflow smoke test without touching the VMs:

```sh
oc -n my-vm-project exec aiops-workflow -- env PYTHONPATH=/tmp/pydeps \
  python3 /opt/workflow/workflow.py --demo
```

Open the MLflow console link → workspace `my-vm-project` → experiment
`aiops-workflow` → Traces. For an intentional end-to-end recovery test on a
demo VM, use `python3 4_chaos_injector/chaos_injector.py --scenario kill-service
--vm rhel10-service-2` (one command). Confirm the EDA rule fired, a Controller
job succeeded with **only that host** as its limit, and telemetry resumed.
Recreate experiments, activation history and service data separately if needed;
none of them are contained in the inventory.
