#!/usr/bin/env python3
"""Generate (or verify) workflow-pod.yaml with the workflow code embedded.

The pod runs the code from a ConfigMap, so every change to the Python files
in this directory must be mirrored into the manifest. This script makes the
mirroring mechanical:

    python3 3_langgraph_workflow/build_pod_manifest.py            # write
    python3 3_langgraph_workflow/build_pod_manifest.py --check    # verify

--check exits non-zero when the checked-in manifest differs from the sources,
which is exactly the "cluster silently runs stale code" trap.
"""

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# Every file embedded in the ConfigMap. Keep in sync with the pod volumeMount.
WORKFLOW_FILES = [
    "state.py",
    "llm.py",
    "mcp_client.py",
    "mock_mcp_server.py",
    "collector.py",
    "anomaly_detector.py",
    "root_cause_analyzer.py",
    "remediation_selector.py",
    "graph.py",
    "workflow.py",
]

LANGGRAPH_PIN = "langgraph>=1.0"
OTEL_PROTO_PIN = "opentelemetry-proto==1.41.1"
NAMESPACE = "my-vm-project"
IMAGE = "registry.redhat.io/ubi9/python-312:latest"


def _block(content, indent="    "):
    """Render file content as a YAML literal block."""
    lines = []
    for line in content.splitlines():
        lines.append(indent + line if line.strip() else "")
    return "\n".join(lines)


def build_manifest():
    config_map_entries = []
    volume_items = []
    for name in WORKFLOW_FILES:
        with open(os.path.join(HERE, name), encoding="utf-8") as handle:
            content = handle.read()
        if not content.endswith("\n"):
            content += "\n"
        config_map_entries.append(f"  {name}: |\n{_block(content)}")
        volume_items.append(
            f"          - key: {name}\n            path: {name}"
        )

    config_map = "\n".join(config_map_entries)
    items = "\n".join(volume_items)
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: aiops-workflow
  namespace: {NAMESPACE}
data:
{config_map}
---
apiVersion: v1
kind: Service
metadata:
  name: aiops-workflow
  namespace: {NAMESPACE}
spec:
  selector:
    app: aiops-workflow
  ports:
    - name: otlp-http
      port: 4318
      targetPort: 4318
---
apiVersion: route.openshift.io/v1
kind: Route
metadata:
  name: aiops-workflow
  namespace: {NAMESPACE}
spec:
  to:
    kind: Service
    name: aiops-workflow
  port:
    targetPort: otlp-http
  tls:
    termination: edge
    insecureEdgeTerminationPolicy: Redirect
---
apiVersion: v1
kind: Pod
metadata:
  name: aiops-workflow
  namespace: {NAMESPACE}
  labels:
    app: aiops-workflow
spec:
  restartPolicy: Always
  securityContext:
    runAsNonRoot: true
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: workflow
      image: {IMAGE}
      imagePullPolicy: IfNotPresent
      securityContext:
        allowPrivilegeEscalation: false
        runAsNonRoot: true
        capabilities:
          drop: ["ALL"]
        seccompProfile:
          type: RuntimeDefault
      command: ["/bin/bash", "-c"]
      args:
        - |
          set -euo pipefail
          pip install --no-cache-dir --target /tmp/pydeps '{LANGGRAPH_PIN}' '{OTEL_PROTO_PIN}'
          export PYTHONPATH=/tmp/pydeps
          exec python3 /opt/workflow/workflow.py
      env:
        - name: HOME
          value: /tmp
        - name: LLM_BASE_URL
          value: ""
        - name: LLM_MODEL
          value: ""
        - name: LLM_API_KEY
          value: ""
        - name: LINUX_MCP_URL
          value: http://linux-mcp.my-vm-project.svc.cluster.local:8000/mcp
        - name: ANSIBLE_MCP_URL
          value: ""
        - name: MCP_INSECURE_TLS
          value: "1"
        - name: WINDOW_SECONDS
          value: "120"
        - name: SILENCE_SECONDS
          value: "10"
      ports:
        - name: otlp-http
          containerPort: 4318
      volumeMounts:
        - name: code
          mountPath: /opt/workflow
  volumes:
    - name: code
      configMap:
        name: aiops-workflow
        items:
{items}
"""


def main():
    parser = argparse.ArgumentParser(description="Generate workflow-pod.yaml")
    parser.add_argument("--output", default=os.path.join(HERE, "workflow-pod.yaml"))
    parser.add_argument("--check", action="store_true",
                        help="compare the sources with the checked-in manifest")
    args = parser.parse_args()

    manifest = build_manifest()
    if args.check:
        try:
            with open(args.output, encoding="utf-8") as handle:
                current = handle.read()
        except OSError:
            raise SystemExit("workflow-pod.yaml is missing; run without --check to create it")
        if current != manifest:
            raise SystemExit(
                "workflow-pod.yaml is out of sync with the Python sources; "
                "re-run build_pod_manifest.py"
            )
        print("workflow-pod.yaml: in sync with sources", flush=True)
        return
    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write(manifest)
    # Validate the generated YAML parses and the ConfigMap keys match the volume.
    try:
        import yaml
    except ImportError:
        print("workflow-pod.yaml: written (PyYAML not available to validate)", flush=True)
        return
    documents = list(yaml.safe_load_all(manifest))
    config_map = documents[0]
    pod = documents[-1]
    keys = set(config_map["data"])
    mounted = {item["key"] for item in pod["spec"]["volumes"][0]["configMap"]["items"]}
    if keys != mounted:
        raise SystemExit(f"manifest validation failed: {keys} != {mounted}")
    print(f"workflow-pod.yaml: written and validated ({len(keys)} files embedded)", flush=True)


if __name__ == "__main__":
    main()
