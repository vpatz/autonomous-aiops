"""Optional MLflow spans for an incident, its agent invocations and its LLM calls."""

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import logging
import os
import threading

_root = ContextVar("aiops_mlflow_root", default=None)
_lock = threading.Lock()
_ready = False
_log = logging.getLogger(__name__)


def _client():
    global _ready
    import mlflow

    with _lock:
        if not _ready:
            mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "aiops-workflow"))
            _ready = True
    return mlflow


@contextmanager
def span(name, inputs, *, workflow=False):
    """Instrumentation errors never suppress or repeat workflow actions."""
    outputs = {}
    parent = _root.get()
    live = None
    if os.environ.get("MLFLOW_TRACKING_URI") and (workflow or parent is not None):
        try:
            live = _client().start_span_no_context(
                name=name, span_type="CHAIN" if workflow else "AGENT",
                parent_span=None if workflow else parent, inputs=inputs,
                tags={"aiops.vm": inputs["service"]} if workflow else None,
            )
        except Exception as exc:
            _log.warning("MLflow could not start %s (%s)", name, type(exc).__name__)
    token = _root.set(live) if live is not None else None
    try:
        yield outputs
    except BaseException as exc:
        outputs["error_type"] = type(exc).__name__
        raise
    finally:
        if token is not None:
            _root.reset(token)
        if live is not None:
            try:
                failed = "error_type" in outputs or outputs.get("status") == "failed"
                live.end(outputs=outputs, status="ERROR" if failed else "OK")
            except Exception as exc:
                _log.warning("MLflow could not finish %s (%s)", name, type(exc).__name__)


@contextmanager
def llm_span(service, phase, inputs=None):
    """One LLM call nested under the current agent span.

    Shows the call's live status ("in-flight" until it settles), duration,
    finish reason, token usage and the prompt messages (the caller truncates
    them to the span budget; MLflow caps span tags at 8000 characters).
    """
    outputs = {"service": service, "phase": phase}
    outputs.update(inputs or {})
    parent = _root.get()
    live = None
    if os.environ.get("MLFLOW_TRACKING_URI") and parent is not None:
        try:
            live = _client().start_span_no_context(
                name=f"llm {phase}", span_type="LLM", parent_span=parent,
                inputs=dict(outputs), tags={"aiops.vm": service} if service else None,
            )
        except Exception as exc:
            _log.warning("MLflow could not start llm %s (%s)", phase, type(exc).__name__)
    try:
        yield outputs
    except BaseException as exc:
        outputs.setdefault("status", "failed")
        outputs["error_type"] = type(exc).__name__
        raise
    finally:
        if live is not None:
            try:
                failed = ("error_type" in outputs
                          or str(outputs.get("status", "")).startswith("failed"))
                live.end(outputs=outputs, status="ERROR" if failed else "OK")
            except Exception as exc:
                _log.warning("MLflow could not finish llm %s (%s)", phase, type(exc).__name__)


def agent(name):
    def decorate(node):
        @wraps(node)
        def traced(state):
            # Record decisions, not raw state (journal output or credentials);
            # LLM prompts are captured in their own nested spans instead.
            inputs = {"service": state["service_name"], "iteration": state.get("iteration", 0),
                      "attempts": state.get("attempts", 0)}
            with span(name, inputs) as out:
                command = node(state)
                update = command.update
                out["next"] = command.goto
                for key in ("status", "iteration", "attempts"):
                    if key in update:
                        out[key] = update[key]
                if "anomaly" in update:
                    out["signals"] = update["anomaly"].get("signals", [])
                if "investigation" in update:
                    out["tools"] = [item.get("tool") for item in update["investigation"]]
                if "root_cause" in update:
                    out["component"] = update["root_cause"].get("component")
                    out["confidence"] = update["root_cause"].get("confidence")
                if "remediation" in update:
                    out["plan"] = [step.get("tool") for step in update["remediation"]["plan"]]
                if "remediation_result" in update:
                    out["accepted"] = update["remediation_result"].get("ok")
                return command
        return traced
    return decorate
