"""Print a read-only inventory of the OpenShift AIOps demo on the current oc context.

Output fields are allowlisted. Do not replace the projections with raw resources:
even ordinary Pod specs and last-applied annotations can contain credentials.
"""
import json
import subprocess
from datetime import datetime, timezone


def get(*args):
    return json.loads(subprocess.check_output(["oc", "get", *args, "-o", "json",
                                               "--request-timeout=20s"]))


def items(*args):
    return sorted(get(*args)["items"], key=lambda x: (x["metadata"].get("namespace", ""),
                                                       x["metadata"]["name"]))


def name(obj):
    return obj["metadata"]["name"]


def conditions(obj):
    return {c["type"]: c["status"] for c in obj.get("status", {}).get("conditions", [])}


def main():
    ns = ("-n", "my-vm-project")
    cv = get("clusterversion", "version")
    infra = get("infrastructure.config.openshift.io", "cluster")["status"]
    ds = get("datasource", "rhel10", "-n", "openshift-virtualization-os-images")
    dsc = get("datasciencecluster", "default-dsc")
    ml = get("mlflows.mlflow.opendatahub.io", "mlflow")
    pod = get("pod", "aiops-workflow", *ns)
    vm_names = [v for v in items("vm", *ns)]
    snapshot = {
        "captured_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "openshift": {"version": cv["status"]["desired"]["version"], "channel": cv["spec"]["channel"],
                      "platform": infra["platformStatus"]["type"], "topology": infra["controlPlaneTopology"],
                      "ingress_domain": get("ingress.config.openshift.io", "cluster")["spec"]["domain"]},
        "nodes": [{"name": name(n), "allocatable_cpu": n["status"]["allocatable"]["cpu"],
                   "allocatable_memory": n["status"]["allocatable"]["memory"],
                   "architecture": n["status"]["nodeInfo"]["architecture"],
                   "roles": sorted(k.split("/")[-1] for k in n["metadata"]["labels"]
                                   if k.startswith("node-role.kubernetes.io/")),
                   "ready": conditions(n).get("Ready")} for n in items("node")],
        "subscriptions": [{"namespace": x["metadata"]["namespace"], "package": x["spec"]["name"],
                           "channel": x["spec"]["channel"], "catalog": x["spec"]["source"],
                           "installed_csv": x["status"].get("installedCSV")} for x in items("sub", "-A")],
        "storage_classes": [{"name": name(x), "provisioner": x["provisioner"],
                             "binding": x.get("volumeBindingMode"),
                             "default": x["metadata"].get("annotations", {}).get(
                                 "storageclass.kubernetes.io/is-default-class") == "true"} for x in items("sc")],
        "image_source": {"name": name(ds), "namespace": ds["metadata"]["namespace"],
                         "source_kind": list(ds["spec"]["source"]), "ready": conditions(ds).get("Ready")},
        "platform_conditions": {
            "hyperconverged": conditions(get("hco", "kubevirt-hyperconverged", "-n", "openshift-cnv")),
            "aap": conditions(get("ansibleautomationplatform", "aap", "-n", "aap")),
            "default-dsc": conditions(dsc),
            "mlflow": conditions(ml),
        },
        "mlflow": {"version": ml["status"].get("version"), "address": ml["status"]["address"]["url"],
                   "pvc": get("pvc", "mlflow-pvc", "-n", "opendatahub")["status"]["phase"]},
        "demo": {
            "namespace": ns[1], "workflow_pod": {"phase": pod["status"]["phase"],
                "ready": conditions(pod).get("Ready"), "service_account": pod["spec"]["serviceAccountName"],
                "secret_refs": sorted({e["valueFrom"]["secretKeyRef"]["name"]
                                       for c in pod["spec"]["containers"] for e in c.get("env", [])
                                       if "secretKeyRef" in e.get("valueFrom", {})})},
            "vms": [{"name": name(v), "state": v["status"].get("printableStatus"),
                     "ready": conditions(v).get("Ready"),
                     "volumes": [t["metadata"]["name"] for t in v["spec"]["dataVolumeTemplates"]]}
                    for v in vm_names],
            "pvcs": [{"name": name(x), "phase": x["status"]["phase"], "storage_class": x["spec"]["storageClassName"],
                      "capacity": x["status"].get("capacity", {}).get("storage")} for x in items("pvc", *ns)],
            "services": {name(x): [p["port"] for p in x["spec"]["ports"]] for x in items("svc", *ns)},
            "routes": {name(x): x["spec"]["host"] for x in items("route", *ns)},
            "deployments": {name(x): x["status"].get("availableReplicas", 0) for x in items("deploy", *ns)},
        },
    }
    print(json.dumps(snapshot, indent=2))


if __name__ == "__main__":
    main()
