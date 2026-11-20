#!/usr/bin/env bash
# Create (or replace) the llm-config Secret the workflow pod reads its LLM
# configuration from, then recreate the pod so it picks the Secret up.
#
# Values come from the environment; this repo's set_secrets_in_shell.sh is
# sourced automatically when present (it is gitignored and never committed):
#
#   export LLM_BASE_URL='https://<llm-host>/v1'
#   export LLM_MODEL='<model>'
#   export LLM_API_KEY='<token>'
#
# Usage: ./set_secrets_in_cluster.sh
set -euo pipefail

NAMESPACE="${NAMESPACE:-my-vm-project}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
if [ -f "$SCRIPT_DIR/set_secrets_in_shell.sh" ]; then
  . "$SCRIPT_DIR/set_secrets_in_shell.sh"
fi

: "${LLM_BASE_URL:?not set - export it or create set_secrets_in_shell.sh}"
: "${LLM_MODEL:?not set - export it or create set_secrets_in_shell.sh}"
: "${LLM_API_KEY:?not set - export it or create set_secrets_in_shell.sh}"

# oc requires key=value literals: a bare --from-literal=KEY does NOT read
# the environment variable of that name.
oc delete secret llm-config -n "$NAMESPACE" --ignore-not-found >/dev/null
oc create secret generic llm-config -n "$NAMESPACE" \
  --from-literal=LLM_BASE_URL="$LLM_BASE_URL" \
  --from-literal=LLM_MODEL="$LLM_MODEL" \
  --from-literal=LLM_API_KEY="$LLM_API_KEY"

# The pod reads the Secret through optional secretKeyRef env vars, so it
# must be recreated to pick a new or changed Secret up.
oc delete pod aiops-workflow -n "$NAMESPACE" --wait=true
oc apply -f "$SCRIPT_DIR/3_langgraph_workflow/workflow-pod.yaml"
