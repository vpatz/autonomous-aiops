#!/usr/bin/env python3
"""Post a restart event for a service.

Set EDA_EVENT_URL to the event endpoint. Run it from the project virtualenv:

    .venv/bin/python 2_aiops_heuristic/eda/send_restart_event.py
"""

import argparse
import base64
import json
import os
import ssl
import urllib.error
import urllib.request

DEFAULT_URL = os.environ.get("EDA_EVENT_URL", "")
DEFAULT_USERNAME = os.environ.get("EDA_EVENT_USERNAME", "aiops-demo")
DEFAULT_PASSWORD = os.environ.get("EDA_EVENT_PASSWORD", "")


def send_event(url, username, password, action, service_name=""):
    if not url:
        raise OSError("Set EDA_EVENT_URL")
    body_json = {"action": action}
    if service_name:
        body_json["service"] = service_name
    payload = json.dumps(body_json).encode()
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    request = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(
        request, timeout=30, context=ssl._create_unverified_context()
    ) as response:
        return response.status, response.read().decode()


def main():
    parser = argparse.ArgumentParser(description="Send a restart event")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--username", default=DEFAULT_USERNAME)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--action", default="restart")
    args = parser.parse_args()
    if not args.password:
        raise SystemExit("Set EDA_EVENT_PASSWORD or pass --password")
    try:
        status, body = send_event(args.url, args.username, args.password, args.action)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"event failed: HTTP {exc.code} {exc.reason}") from exc
    except (OSError, urllib.error.URLError) as exc:
        raise SystemExit(f"event failed: {exc}") from exc
    print(f"event accepted: HTTP {status}")
    if body.strip():
        print(body)


if __name__ == "__main__":
    main()
