#!/usr/bin/env python3
"""Step 2: decide whether a service needs a restart from the metrics it sent.

A service is up when its latest export is newer than the silence window.
A service that was seen and then goes quiet is down, and the restart action
is returned once per cooldown.

Verify:

    python3 2_aiops_agent/analyze_metrics.py --self-check
"""

from dataclasses import dataclass

STEP = "step 2 analyze metrics"
SILENCE_SECONDS = 10
COOLDOWN_SECONDS = 60


@dataclass
class Analysis:
    service_name: str
    status: str
    silent_for: float
    action: str
    reason: str


def analyze(exports, now, silence_seconds, cooldown_seconds, last_attempts):
    decisions = []
    for service_name, export in exports.items():
        silent_for = now - export.received_at
        if silent_for < silence_seconds:
            decisions.append(
                Analysis(
                    service_name,
                    "up",
                    silent_for,
                    "",
                    "metrics are arriving",
                )
            )
            continue
        since_attempt = now - last_attempts.get(service_name, 0.0)
        action = "restart" if since_attempt >= cooldown_seconds else ""
        decisions.append(
            Analysis(
                service_name,
                "down",
                silent_for,
                action,
                f"no metrics for {silent_for:.0f}s",
            )
        )
    return decisions


def log_analysis(decision):
    action = decision.action or "none"
    print(
        f"{STEP}: service={decision.service_name} status={decision.status} "
        f"silent_for={decision.silent_for:.0f}s action={action} reason={decision.reason}",
        flush=True,
    )


class _Export:
    def __init__(self, service_name, received_at):
        self.service_name = service_name
        self.received_at = received_at


def self_check():
    now = 1000.0
    fresh = {"vm-1": _Export("vm-1", 995.0)}
    up = analyze(fresh, now, SILENCE_SECONDS, COOLDOWN_SECONDS, {})
    if len(up) != 1 or up[0].status != "up" or up[0].action:
        raise SystemExit(f"{STEP}: fresh metrics should be up, got {up}")

    stale = {"vm-1": _Export("vm-1", 980.0)}
    down = analyze(stale, now, SILENCE_SECONDS, COOLDOWN_SECONDS, {})
    if down[0].status != "down" or down[0].action != "restart":
        raise SystemExit(f"{STEP}: stale metrics should restart, got {down}")

    cooling = analyze(stale, now, SILENCE_SECONDS, COOLDOWN_SECONDS, {"vm-1": 990.0})
    if cooling[0].status != "down" or cooling[0].action:
        raise SystemExit(f"{STEP}: cooldown should suppress restart, got {cooling}")

    if analyze({}, now, SILENCE_SECONDS, COOLDOWN_SECONDS, {}):
        raise SystemExit(f"{STEP}: unknown services must not be restarted")

    print(f"{STEP}: self-check ok", flush=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Step 2: analyze received metrics")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not args.self_check:
        raise SystemExit("pass --self-check; the agent calls analyze() with live exports")
    self_check()


if __name__ == "__main__":
    main()
