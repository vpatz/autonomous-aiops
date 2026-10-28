#!/usr/bin/env python3
"""Send an event to the AIOps demo Event-Driven Ansible stream on AAP.

The rulebook runs the "Restart AIOps demo app" job when the payload
action is "restart". Run it from the project virtualenv:

    .venv/bin/python sample-workload/eda/send_restart_event.py
"""

import argparse
import base64
import http.client
import json
import os
import socket
import ssl
import urllib.error
import urllib.parse

DEFAULT_URL = os.environ.get(
    "EDA_EVENT_URL",
    "https://rhel-10-node.local/api/eda/v1/external_event_stream/"
    "980e5717-6b88-4f33-b4a1-4295c77504e0/post/",
)
DEFAULT_USERNAME = os.environ.get("EDA_EVENT_USERNAME", "aiops-demo")
DEFAULT_PASSWORD = os.environ.get("EDA_EVENT_PASSWORD", "")

# launchd forwards this to the VM gateway. Cursor-spawned processes get
# errno 65 if they open 192.168.64.23 themselves.
GATEWAY_PROXY = ("127.0.0.1", 18443)


def _open_vm_socket(host, port):
    try:
        return socket.create_connection((host, port), timeout=30), None
    except OSError as exc:
        if exc.errno not in (51, 64, 65) or port != 443:
            raise
    try:
        return socket.create_connection(GATEWAY_PROXY, timeout=30), None
    except OSError as exc:
        raise OSError(
            f"cannot reach {host}:{port} ({exc}); start sample-workload/gateway_local_proxy.py via launchd"
        ) from exc


def send_event(url, username, password, action, service_name=""):
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname
    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    body_json = {"action": action}
    if service_name:
        body_json["service"] = service_name
    payload = json.dumps(body_json).encode()
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    raw, _relay = _open_vm_socket(host, port)
    tls = ssl._create_unverified_context().wrap_socket(raw, server_hostname=host)
    conn = http.client.HTTPSConnection(host, port, timeout=30)
    conn.sock = tls
    try:
        conn.request(
            "POST",
            path,
            body=payload,
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
                "Host": host,
            },
        )
        response = conn.getresponse()
        body = response.read().decode()
        if response.status >= 400:
            raise urllib.error.HTTPError(url, response.status, body, hdrs=None, fp=None)
        return response.status, body
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Send an EDA event to AAP")
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
    except (OSError, urllib.error.URLError, http.client.HTTPException) as exc:
        raise SystemExit(f"event failed: {exc}") from exc
    print(f"event accepted: HTTP {status}")
    if body.strip():
        print(body)


if __name__ == "__main__":
    main()
