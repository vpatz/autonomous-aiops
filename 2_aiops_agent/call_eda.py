#!/usr/bin/env python3
"""Step 3: post a restart event to Event-Driven Ansible.

Verify the request that would be sent, without contacting AAP:

    python3 2_aiops_agent/call_eda.py --dry-run

Send a real event:

    EDA_EVENT_PASSWORD=... python3 2_aiops_agent/call_eda.py --service rhel10-service-1
"""

import json
import sys
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "eda"))

from send_restart_event import DEFAULT_PASSWORD, DEFAULT_URL, DEFAULT_USERNAME, send_event

STEP = "step 3 call eda"


def event_payload(action, service_name):
    payload = {"action": action}
    if service_name:
        payload["service"] = service_name
    return payload


def call_eda(url, username, password, action, service_name="", dry_run=False):
    payload = event_payload(action, service_name)
    print(
        f"{STEP}: service={service_name or '-'} action={action} url={url} "
        f"payload={json.dumps(payload)}",
        flush=True,
    )
    if dry_run:
        print(f"{STEP}: dry-run ok", flush=True)
        return 0, "dry-run"
    status, body = send_event(url, username, password, action, service_name)
    print(f"{STEP}: accepted HTTP {status}", flush=True)
    if body.strip():
        print(body, flush=True)
    return status, body


def self_check():
    payload = event_payload("restart", "rhel10-service-1")
    if payload != {"action": "restart", "service": "rhel10-service-1"}:
        raise SystemExit(f"{STEP}: unexpected payload {payload}")
    status, body = call_eda(
        DEFAULT_URL,
        DEFAULT_USERNAME,
        "self-check",
        "restart",
        "rhel10-service-1",
        dry_run=True,
    )
    if status != 0 or body != "dry-run":
        raise SystemExit(f"{STEP}: dry-run returned {status} {body}")
    print(f"{STEP}: self-check ok", flush=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Step 3: call Event-Driven Ansible")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--action", default="restart")
    parser.add_argument("--service", default="")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    if args.dry_run:
        call_eda(args.url, args.username, args.password, args.action, args.service, dry_run=True)
        return
    if not args.password:
        raise SystemExit("Set EDA_EVENT_PASSWORD or pass --password")
    try:
        call_eda(args.url, args.username, args.password, args.action, args.service)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"{STEP}: failed HTTP {exc.code} {exc.reason}") from exc
    except (OSError, urllib.error.URLError) as exc:
        raise SystemExit(f"{STEP}: failed {exc}") from exc


if __name__ == "__main__":
    main()
