#!/usr/bin/env python3
"""Agent 1: anomaly detector.

Examines a time window of telemetry (metrics, logs, traces) for one service
and decides whether an unknown error state is present. Deterministic
heuristics make the decision; a configured LLM only phrases the summary, so
the trigger behavior is identical with or without a model.

Triggers:
- metrics-silent:      the service was seen but stopped sending telemetry
- dummy-crash-metric:  the demo fault counter incremented
- error-logs:          OTel log records at ERROR severity or higher
- failed-spans:        trace spans with ERROR status
- cpu-saturation / memory-pressure: host gauges above threshold

Verify:

    python3 3_langgraph_workflow/anomaly_detector.py --self-check
"""

import argparse

import llm
import state

STEP = "agent 1 anomaly detector"
CPU_THRESHOLD_PERCENT = 90.0
MEMORY_THRESHOLD_PERCENT = 90.0
ERROR_SEVERITY_NUMBER = 17  # OTel SeverityNumbers: 17=ERROR and above


def _is_error_log(entry):
    if entry.get("severity_number", 0) >= ERROR_SEVERITY_NUMBER:
        return True
    return entry.get("severity_text", "").upper() in ("ERROR", "CRITICAL", "FATAL")


def _is_failed_span(span):
    return span.get("status_code") == 2


def analyze_window(window, silence_seconds=state.DEFAULT_SILENCE_SECONDS):
    """Return the anomaly dict for one telemetry window (pure function)."""
    signals = []
    reasons = []
    metrics = window.get("metrics", {})
    logs = window.get("logs", [])
    traces = window.get("traces", [])
    last_seen = window.get("last_seen_seconds_ago")

    if window.get("ever_seen") and last_seen is not None and last_seen > silence_seconds:
        signals.append("metrics-silent")
        reasons.append(f"no telemetry for {last_seen:.0f}s (silence threshold {silence_seconds:.0f}s)")
    events = window.get("metric_events")
    if events is not None:
        # OTel counters are cumulative: the fault signal is the increment
        # inside the window, not the counter's presence. A restart that
        # clears the fault produces no further increments.
        crash_events = [item for item in events if item.get("metric") == "dummy-crash"]
        if crash_events:
            signals.append("dummy-crash-metric")
            reasons.append(f"dummy-crash counter incremented to {crash_events[-1].get('to')}")
    elif metrics.get("dummy-crash", 0) > 0:
        signals.append("dummy-crash-metric")
        reasons.append(f"dummy-crash counter at {metrics.get('dummy-crash')}")
    error_logs = [entry for entry in logs if _is_error_log(entry)]
    if error_logs:
        signals.append("error-logs")
        reasons.append(f"{len(error_logs)} ERROR log record(s), latest: "
                       f"{error_logs[-1].get('body', '')[:80]}")
    failed_spans = [span for span in traces if _is_failed_span(span)]
    if failed_spans:
        signals.append("failed-spans")
        reasons.append(f"{len(failed_spans)} failed span(s): "
                       f"{failed_spans[-1].get('name', '')}")
    cpu = metrics.get("service.cpu.load")
    if cpu is not None and cpu > CPU_THRESHOLD_PERCENT:
        signals.append("cpu-saturation")
        reasons.append(f"service.cpu.load at {cpu:.1f}%")
    memory = metrics.get("service.memory.load")
    if memory is not None and memory > MEMORY_THRESHOLD_PERCENT:
        signals.append("memory-pressure")
        reasons.append(f"service.memory.load at {memory:.1f}%")
    uptime = metrics.get("service.uptime")
    if (cpu == 0.0 and memory == 0.0
            and isinstance(uptime, (int, float)) and uptime > 60):
        # A running host cannot report exactly zero CPU and zero memory load
        # past startup; the chaos injector's fake-telemetry scenario does.
        signals.append("zero-telemetry")
        reasons.append("telemetry reports impossible zero CPU and memory load")

    detected = bool(signals)
    score = len(signals) + (1 if "metrics-silent" in signals else 0)
    if not detected:
        severity = "none"
    elif "metrics-silent" in signals or score >= 3:
        severity = "high"
    elif score == 2:
        severity = "medium"
    else:
        severity = "low"
    summary = "; ".join(reasons) if reasons else "telemetry within normal bounds"
    return {
        "detected": detected,
        "signals": signals,
        "reasons": reasons,
        "score": score,
        "severity": severity,
        "summary": summary,
    }


def _llm_summary(service_name, anomaly):
    """Ask the LLM for a one-sentence incident summary (best effort)."""
    prompt = (
        "You are an ITOps anomaly detector. Summarize the anomaly for a service "
        "in one or two sentences for an operations dashboard. Reply with only a "
        "JSON object like {\"summary\": \"...\"}.\n\n"
        f"Service: {service_name}\n"
        f"Signals: {', '.join(anomaly['signals'])}\n"
        f"Details: {'; '.join(anomaly['reasons'])}\n"
    )
    reply = llm.chat_json([{"role": "user", "content": prompt}])
    summary = reply.get("summary")
    return summary if isinstance(summary, str) and summary.strip() else None


def build_node(silence_seconds=state.DEFAULT_SILENCE_SECONDS, use_llm=True):
    """Return the LangGraph node for the anomaly detector."""

    def detector_node(workflow_state):
        from langgraph.types import Command

        service_name = workflow_state["service_name"]
        anomaly = analyze_window(workflow_state.get("telemetry", {}), silence_seconds)
        attempts = workflow_state.get("attempts", 0)
        max_attempts = workflow_state.get("max_attempts", state.DEFAULT_MAX_ATTEMPTS)
        finding = (
            f"[{STEP}] service={service_name} anomaly={anomaly['detected']} "
            f"severity={anomaly['severity']} signals={','.join(anomaly['signals']) or '-'} "
            f"attempts={attempts}/{max_attempts}"
        )
        if not anomaly["detected"]:
            status = state.STATUS_REMEDIATED if attempts else state.STATUS_HEALTHY
            if attempts:
                finding = f"[{STEP}] verification passed: telemetry back to normal, remediation confirmed"
            update = {"anomaly": anomaly, "status": status, "findings": [finding]}
            return Command(goto="__end__", update=update)
        if use_llm and llm.available():
            try:
                summary = _llm_summary(service_name, anomaly)
                if summary:
                    anomaly = {**anomaly, "summary": summary}
            except Exception as exc:  # noqa: BLE001 - LLM is best effort
                finding += f" (llm summary failed: {exc})"[:200]
        if attempts >= max_attempts:
            update = {
                "anomaly": anomaly,
                "status": state.STATUS_FAILED,
                "findings": [finding + " -> giving up: max remediation attempts reached"],
            }
            return Command(goto="__end__", update=update)
        update = {"anomaly": anomaly, "status": state.STATUS_INVESTIGATING, "findings": [finding]}
        return Command(goto=state.NODE_RCA, update=update)

    return detector_node


def self_check():
    healthy = {
        "ever_seen": True,
        "last_seen_seconds_ago": 2.0,
        "metrics": {"service.uptime": 10.0, "service.cpu.load": 5.0},
        "logs": [],
        "traces": [],
    }
    anomaly = analyze_window(healthy)
    if anomaly["detected"] or anomaly["severity"] != "none":
        raise SystemExit(f"{STEP}: healthy window flagged {anomaly}")

    crashed = {
        "ever_seen": True,
        "last_seen_seconds_ago": 3.0,
        "metrics": {"service.uptime": 187.0, "dummy-crash": 1},
        "metric_events": [{"metric": "dummy-crash", "from": 0, "to": 1, "age_seconds": 3.0}],
        "logs": [
            {"severity_number": 17, "severity_text": "ERROR", "body": "simulated a dummy crash"},
            {"severity_number": 9, "severity_text": "INFO", "body": "page served"},
        ],
        "traces": [{"name": "dummy-crash", "status_code": 2, "status_message": "dummy crash"}],
    }
    anomaly = analyze_window(crashed)
    for expected in ("dummy-crash-metric", "error-logs", "failed-spans"):
        if expected not in anomaly["signals"]:
            raise SystemExit(f"{STEP}: missing signal {expected} in {anomaly['signals']}")
    if not anomaly["detected"] or anomaly["severity"] != "high":
        raise SystemExit(f"{STEP}: crash window not flagged high {anomaly}")

    # Post-remediation shape: the counter is still present (cumulative) but
    # no increment happened inside the window -> no fault signal.
    cleared = {
        "ever_seen": True,
        "last_seen_seconds_ago": 1.0,
        "metrics": {"service.uptime": 12.0, "dummy-crash": 1},
        "metric_events": [],
        "logs": [],
        "traces": [],
    }
    anomaly = analyze_window(cleared)
    if anomaly["detected"]:
        raise SystemExit(f"{STEP}: cleared window must not flag the stale counter {anomaly}")

    # Windows without metric_events (synthetic shapes) fall back to the raw value.
    legacy = {
        "ever_seen": True,
        "last_seen_seconds_ago": 2.0,
        "metrics": {"dummy-crash": 1},
        "logs": [],
        "traces": [],
    }
    if "dummy-crash-metric" not in analyze_window(legacy)["signals"]:
        raise SystemExit(f"{STEP}: legacy raw-value window should flag the counter")

    silent = {"ever_seen": True, "last_seen_seconds_ago": 42.0, "metrics": {}, "logs": [], "traces": []}
    anomaly = analyze_window(silent)
    if "metrics-silent" not in anomaly["signals"] or anomaly["severity"] != "high":
        raise SystemExit(f"{STEP}: silent window not flagged {anomaly}")

    unknown = {"ever_seen": False, "last_seen_seconds_ago": None, "metrics": {}, "logs": [], "traces": []}
    if analyze_window(unknown)["detected"]:
        raise SystemExit(f"{STEP}: never-seen service must not be flagged")

    saturated = {
        "ever_seen": True,
        "last_seen_seconds_ago": 1.0,
        "metrics": {"service.cpu.load": 97.5, "service.memory.load": 95.0},
        "logs": [],
        "traces": [],
    }
    anomaly = analyze_window(saturated)
    if "cpu-saturation" not in anomaly["signals"] or "memory-pressure" not in anomaly["signals"]:
        raise SystemExit(f"{STEP}: saturation window not flagged {anomaly}")

    # Fake telemetry: impossible zeros past startup must be flagged, but a
    # service that just booted (small uptime) may legitimately report zeros.
    corrupted = {
        "ever_seen": True,
        "last_seen_seconds_ago": 1.0,
        "metrics": {"service.uptime": 999.0, "service.cpu.load": 0.0,
                    "service.memory.load": 0.0},
        "logs": [],
        "traces": [],
    }
    anomaly = analyze_window(corrupted)
    if "zero-telemetry" not in anomaly["signals"]:
        raise SystemExit(f"{STEP}: zero-telemetry window not flagged {anomaly}")
    booting = {
        "ever_seen": True,
        "last_seen_seconds_ago": 1.0,
        "metrics": {"service.uptime": 5.0, "service.cpu.load": 0.0,
                    "service.memory.load": 0.0},
        "logs": [],
        "traces": [],
    }
    if analyze_window(booting)["detected"]:
        raise SystemExit(f"{STEP}: boot-time zeros must not be flagged")

    # Node-level check: routing on a healthy window ends the workflow.
    from langgraph.types import Command

    node = build_node()
    result = node(state.initial_state("vm-1", telemetry=healthy))
    if not isinstance(result, Command) or result.goto != "__end__":
        raise SystemExit(f"{STEP}: healthy window should end workflow, got {result}")
    result = node(state.initial_state("vm-1", telemetry=crashed))
    if not isinstance(result, Command) or result.goto != state.NODE_RCA:
        raise SystemExit(f"{STEP}: anomaly should route to RCA, got {result}")
    exhausted = {**state.initial_state("vm-1", telemetry=crashed), "attempts": 3, "max_attempts": 3}
    result = node(exhausted)
    if result.goto != "__end__" or result.update.get("status") != state.STATUS_FAILED:
        raise SystemExit(f"{STEP}: exhausted attempts should fail, got {result}")
    print(f"{STEP}: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Agent 1: anomaly detector")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not args.self_check:
        raise SystemExit("pass --self-check; the workflow calls analyze_window() with live windows")
    self_check()


if __name__ == "__main__":
    main()
