#!/usr/bin/env python3
"""Mock MCP server for offline demos and self-checks.

Speaks MCP over stdio (newline-delimited JSON-RPC 2.0), the same protocol
the real linux-mcp and Ansible MCP servers use.

Roles:
- linux:   pretends to be a RHEL VM where the demo service just reported a
           dummy-crash fault (process still running, error state set)
- ansible: pretends to be an Ansible MCP server with an approved-command
           allowlist, and remembers when the service was restarted

Run:

    python3 3_langgraph_workflow/mock_mcp_server.py --role linux
    python3 3_langgraph_workflow/mock_mcp_server.py --role ansible

Verify:

    python3 3_langgraph_workflow/mock_mcp_server.py --self-check
"""

import argparse
import json
import sys

PROTOCOL_VERSION = "2024-11-05"
SERVICE_UNIT = "aiops-demo.service"

# Shared in-process state for the offline demo (--demo) and self-checks.
_ANSIBLE_STATE = {"restarted": False}

APPROVED_MODULES = [
    "ansible.builtin.systemd",
    "ansible.builtin.service_facts",
    "ansible.builtin.command",
    "ansible.builtin.setup",
]


def linux_tools():
    return [
        {
            "name": "run_command",
            "description": "Run a shell command on the managed host and return its output",
            "inputSchema": {
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
            },
        },
        {
            "name": "read_file",
            "description": "Read a file from the managed host",
            "inputSchema": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    ]


def ansible_tools():
    return [
        {
            "name": "list_approved_modules",
            "description": "List the approved (allowlisted) Ansible modules that can be run",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "run_module",
            "description": "Run an approved Ansible module against a host",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "module": {"type": "string"},
                    "arguments": {"type": "object"},
                },
                "required": ["host", "module"],
            },
        },
    ]


def _systemctl_status():
    state = "running" if _ANSIBLE_STATE["restarted"] else "running (fault reported)"
    return (
        f"\u25cf {SERVICE_UNIT} - AIOps demo service\n"
        "     Loaded: loaded (/etc/systemd/system/aiops-demo.service; enabled; preset: enabled)\n"
        f"     Active: active ({state}) since Tue 2026-11-17 10:09:04 UTC; 12min ago\n"
        "   Main PID: 2841 (python3)\n"
        "      Tasks: 4 (limit: 4915)\n"
        "     Memory: 61.2M\n"
        "        CPU: 4.216s\n"
        "     CGroup: /system.slice/aiops-demo.service\n"
        "             \u2514\u25002841 /opt/aiops/venv/bin/python /opt/aiops/src/1_vm_services/service.py\n"
    )


def _journalctl():
    return (
        "-- Boot 6e5a2f9c... --\n"
        "Nov 17 10:09:04 rhel10-service-1 systemd[1]: Started AIOps demo service.\n"
        "Nov 17 10:09:07 rhel10-service-1 python3[2841]: service app will send dummy-crash in 187.4s\n"
        "Nov 17 10:12:14 rhel10-service-1 python3[2841]: service app sent dummy-crash event\n"
        "Nov 17 10:12:14 rhel10-service-1 python3[2841]: ERROR: service app simulated a dummy crash\n"
        "Nov 17 10:12:15 rhel10-service-1 systemd[1]: aiops-demo.service: Deactivated successfully.\n"
    )


def linux_call(name, arguments):
    """Handle a linux-role tool call. Returns text; raises ValueError on bad input."""
    if name == "run_command":
        command = (arguments or {}).get("command", "")
        if "systemctl status" in command and SERVICE_UNIT in command:
            return _systemctl_status()
        if command.strip().startswith("journalctl"):
            return _journalctl()
        if command.strip() == "hostname":
            return "rhel10-service-1"
        if command.strip() in ("uptime", "cat /proc/uptime"):
            return " 10:24:01 up 1:15,  1 user,  load average: 0.31, 0.42, 0.35"
        return f"[mock] command executed: {command}"
    if name == "read_file":
        path = (arguments or {}).get("path", "")
        if path == "/var/log/aiops-setup.log":
            return (
                "Installed:\n  git-2.43.0-1.el10.x86_64\n"
                "Successfully installed pip-25.2\n"
                "Successfully installed flask-3.0.3 opentelemetry-1.41.1\n"
                "Created symlink /etc/systemd/system/multi-user.target.wants/aiops-demo.service\n"
            )
        if path in ("/var/log/messages", "/var/log/secure"):
            return (
                "Nov 17 10:12:14 rhel10-service-1 python3[2841]: service app sent dummy-crash event\n"
                "Nov 17 10:12:14 rhel10-service-1 python3[2841]: ERROR: service app simulated a dummy crash\n"
            )
        return f"[mock] file not found: {path}"
    raise ValueError(f"unknown linux tool {name}")


def service_restarted():
    """True once the mock ansible role has restarted the demo service."""
    return _ANSIBLE_STATE["restarted"]


def reset():
    """Reset the shared in-process mock state (hermetic demo runs)."""
    _ANSIBLE_STATE["restarted"] = False


def ansible_call(name, arguments):
    """Handle an ansible-role tool call. Returns text; raises ValueError on bad input."""
    args = arguments or {}
    if name == "list_approved_modules":
        return "Approved Ansible modules: " + ", ".join(APPROVED_MODULES)
    if name == "run_module":
        module = args.get("module", "")
        if module not in APPROVED_MODULES:
            raise ValueError(f"module '{module}' is not in the approved list")
        host = args.get("host", "unknown")
        module_args = args.get("arguments", {})
        if module == "ansible.builtin.systemd":
            target_state = module_args.get("state", "unknown")
            if target_state in ("restarted", "started"):
                _ANSIBLE_STATE["restarted"] = True
            return json.dumps(
                {
                    "changed": True,
                    "host": host,
                    "name": module_args.get("name", ""),
                    "state": "running" if target_state in ("restarted", "started") else target_state,
                }
            )
        if module == "ansible.builtin.service_facts":
            return json.dumps(
                {
                    "changed": False,
                    "host": host,
                    "services": {SERVICE_UNIT: {"state": "running", "status": "running"}},
                }
            )
        return json.dumps({"changed": False, "host": host, "module": module, "ok": True})
    raise ValueError(f"unknown ansible tool {name}")


def _tool_result(text):
    return {"content": [{"type": "text", "text": text}], "isError": False}


def _tool_error(message):
    return {"content": [{"type": "text", "text": message}], "isError": True}


def serve(role):
    tools = linux_tools() if role == "linux" else ansible_tools()
    handler = linux_call if role == "linux" else ansible_call
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            continue
        method = message.get("method", "")
        if method == "initialize":
            response = {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "result": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": f"mock-{role}-mcp", "version": "1.0.0"},
                },
            }
        elif method == "notifications/initialized":
            continue
        elif method == "tools/list":
            response = {"jsonrpc": "2.0", "id": message.get("id"), "result": {"tools": tools}}
        elif method == "tools/call":
            params = message.get("params", {})
            try:
                response = {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "result": _tool_result(handler(params.get("name"), params.get("arguments") or {})),
                }
            except ValueError as exc:
                response = {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "result": _tool_error(str(exc)),
                }
        elif method == "ping":
            response = {"jsonrpc": "2.0", "id": message.get("id"), "result": {}}
        elif message.get("id") is not None:
            response = {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32601, "message": f"method not found: {method}"},
            }
        else:
            continue
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


def self_check():
    status = linux_call("run_command", {"command": f"systemctl status {SERVICE_UNIT}"})
    if "active (running" not in status:
        raise SystemExit(f"mock linux: status looks wrong: {status[:80]}")
    journal = linux_call("run_command", {"command": "journalctl -u aiops-demo.service"})
    if "dummy crash" not in journal:
        raise SystemExit("mock linux: journal missing dummy crash")
    if "dummy-crash" not in ansible_call("list_approved_modules", {}).replace("dummy-crash", ""):
        pass  # approved modules text has no crash reference; sanity below
    modules = ansible_call("list_approved_modules", {})
    if "ansible.builtin.systemd" not in modules:
        raise SystemExit("mock ansible: approved modules missing systemd")
    result = json.loads(ansible_call("run_module", {"host": "vm-1", "module": "ansible.builtin.systemd", "arguments": {"name": SERVICE_UNIT, "state": "restarted"}}))
    if not result.get("changed") or not service_restarted():
        raise SystemExit("mock ansible: restart did not register")
    try:
        ansible_call("run_module", {"host": "vm-1", "module": "ansible.builtin.shell", "arguments": {}})
        raise SystemExit("mock ansible: unapproved module should be rejected")
    except ValueError:
        pass
    print("mock mcp server: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Mock MCP server over stdio")
    parser.add_argument("--role", choices=["linux", "ansible"], default="linux")
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        self_check()
        return
    serve(args.role)


if __name__ == "__main__":
    main()
