# Test the demo offline

The offline run uses synthetic VM telemetry and mock MCP tools to walk through a stopped service, its investigation, and verified recovery. It runs in the terminal; it does not start the dashboard or require an OpenShift cluster, model endpoint, or Ansible installation.

From the repository root, install the dependencies once (internet access is needed for installation unless the packages are cached):

```sh
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r 3_langgraph_workflow/requirements.txt
```

Run the narrative demo and the built-in checks:

```sh
python3 3_langgraph_workflow/workflow.py --demo
python3 3_langgraph_workflow/workflow.py --self-check
```

Look for `final status: remediated` in the demo and `self-check ok` at the end of the checks. For a local run with actual service telemetry, see the [workflow guide](3_langgraph_workflow/README.md#local-end-to-end-with-real-telemetry).
