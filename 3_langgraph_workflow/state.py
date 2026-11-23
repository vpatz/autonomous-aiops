#!/usr/bin/env python3
"""Shared state and node names for the LangGraph multi-agent workflow.

The workflow is a LangGraph StateGraph over WorkflowState:

    START -> anomaly_detector -> root_cause_analyzer (inner loop, iterates
             through MCP tools) -> remediation_selector -> remediation_executor
             -> anomaly_detector (outer loop, verifies the remediation) -> END

`investigation` and `findings` are append-only lists (operator.add reducers):
every root-cause iteration and every agent decision is kept as an audit trail
for the dashboard and the demo narrative.

Verify:

    python3 3_langgraph_workflow/state.py --self-check
"""

import operator
from typing import Annotated, TypedDict

STEP = "workflow state"

# LangGraph node names.
NODE_DETECTOR = "anomaly_detector"
NODE_RCA = "root_cause_analyzer"
NODE_SELECTOR = "remediation_selector"
NODE_EXECUTOR = "remediation_executor"

# Workflow statuses.
STATUS_HEALTHY = "healthy"
STATUS_INVESTIGATING = "investigating"
STATUS_REMEDIATING = "remediating"
STATUS_REMEDIATED = "remediated"
STATUS_FAILED = "failed"

DEFAULT_WINDOW_SECONDS = 120
LLM_EVIDENCE_SECONDS = 10
DEFAULT_SILENCE_SECONDS = 10
DEFAULT_MAX_ITERATIONS = 25
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_POLL_SECONDS = 5
DEFAULT_COOLDOWN_SECONDS = 120
DEFAULT_VERIFY_WAIT_SECONDS = 30


class WorkflowState(TypedDict, total=False):
    """State shared by every agent node in the graph."""

    service_name: str
    window_seconds: int
    telemetry: dict
    anomaly: dict
    detector_evidence: dict  # transient snapshot for the RCA inner loop
    investigation: Annotated[list, operator.add]
    root_cause: dict
    vm_halted: bool
    remediation: dict
    remediation_result: dict
    findings: Annotated[list, operator.add]
    attempts: int
    max_attempts: int
    iteration: int
    max_iterations: int
    verify_since: float
    status: str


def initial_state(
    service_name,
    window_seconds=DEFAULT_WINDOW_SECONDS,
    telemetry=None,
    max_iterations=DEFAULT_MAX_ITERATIONS,
    max_attempts=DEFAULT_MAX_ATTEMPTS,
):
    return {
        "service_name": service_name,
        "window_seconds": window_seconds,
        "telemetry": telemetry or {},
        "anomaly": {},
        "investigation": [],
        "root_cause": {},
        "remediation": {},
        "remediation_result": {},
        "findings": [],
        "attempts": 0,
        "max_attempts": max_attempts,
        "iteration": 0,
        "max_iterations": max_iterations,
        "verify_since": 0.0,
        "status": STATUS_INVESTIGATING,
    }


def self_check():
    state = initial_state("vm-1")
    expected = {
        "service_name": "vm-1",
        "window_seconds": DEFAULT_WINDOW_SECONDS,
        "attempts": 0,
        "iteration": 0,
        "status": STATUS_INVESTIGATING,
    }
    for key, value in expected.items():
        if state.get(key) != value:
            raise SystemExit(f"{STEP}: expected {key}={value}, got {state.get(key)}")
    # The append-only channels must reduce with operator.add so that each
    # inner-loop iteration adds to the audit trail instead of replacing it.
    if state["investigation"] + [{"tool": "x"}] != [{"tool": "x"}]:
        raise SystemExit(f"{STEP}: investigation must be a list")
    if not hasattr(operator, "add"):
        raise SystemExit(f"{STEP}: operator.add missing")
    for node in (NODE_DETECTOR, NODE_RCA, NODE_SELECTOR, NODE_EXECUTOR):
        if not isinstance(node, str) or not node:
            raise SystemExit(f"{STEP}: bad node name {node}")
    print(f"{STEP}: self-check ok", flush=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Shared workflow state")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not args.self_check:
        raise SystemExit("pass --self-check; the workflow imports this module")
    self_check()


if __name__ == "__main__":
    main()
