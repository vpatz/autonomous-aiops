#!/usr/bin/env python3
"""Minimal MCP client for the multi-agent workflow.

Supports the three transports the demo needs:

- stdio  (StdioMcpClient):  spawn a local MCP server command, e.g. the mock
  server or a locally installed Ansible MCP server
- streamable HTTP (HttpMcpClient): talk to a remote MCP endpoint such as the
  linux-mcp server running in the cluster
- in-process fake (FakeMcpClient): self-checks and the offline demo

Configuration comes from the environment:

    LINUX_MCP_URL / LINUX_MCP_COMMAND         linux (RCA) MCP server
    ANSIBLE_MCP_URL / ANSIBLE_MCP_COMMAND     Ansible (remediation) MCP server
    MCP_INSECURE_TLS=1                        skip TLS verification (lab routes)

Verify:

    python3 3_langgraph_workflow/mcp_client.py --self-check
"""

import argparse
import json
import os
import shlex
import subprocess
import urllib.request

import agent_tracing

PROTOCOL_VERSION = "2024-11-05"


class McpError(Exception):
    pass


def tool_text(result):
    """Extract the text of an MCP tools/call result; raise on isError."""
    parts = []
    for item in (result or {}).get("content", []):
        if item.get("type") == "text":
            parts.append(item.get("text", ""))
    text = "\n".join(parts).strip()
    if (result or {}).get("isError"):
        raise McpError(text or "MCP tool call failed")
    return text


class BaseMcpClient:
    def __init__(self):
        self._id = 0
        self.server_info = {}

    def _next_id(self):
        self._id += 1
        return self._id

    def _initialize(self):
        result = self._request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "aiops-workflow", "version": "1.0.0"},
            },
        )
        self.server_info = (result or {}).get("serverInfo", {})
        self._notify("notifications/initialized")

    def list_tools(self):
        return (self._request("tools/list", {}) or {}).get("tools", [])

    def call_tool(self, name, arguments=None):
        with agent_tracing.tool_span(name, arguments or {}) as record:
            try:
                result = self._request("tools/call", {"name": name, "arguments": arguments or {}})
                text = tool_text(result)
            except McpError as exc:
                record["status"] = "failed"
                record["output"] = agent_tracing.snippet(str(exc))
                raise
            record["status"] = "ok"
            record["output"] = agent_tracing.snippet(text)
            return text

    def _request(self, method, params):
        raise NotImplementedError

    def _notify(self, method):
        raise NotImplementedError

    def close(self):
        return None


class FakeMcpClient(BaseMcpClient):
    """In-process client wired straight to a handler (self-checks, --demo)."""

    def __init__(self, tools, handler):
        super().__init__()
        self._tools = tools
        self._handler = handler
        self._initialize()

    def _request(self, method, params):
        if method == "initialize":
            return {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fake-mcp", "version": "1.0.0"},
            }
        if method == "tools/list":
            return {"tools": self._tools}
        if method == "tools/call":
            try:
                return {
                    "content": [{"type": "text", "text": self._handler(params.get("name"), params.get("arguments") or {})}],
                    "isError": False,
                }
            except ValueError as exc:
                return {"content": [{"type": "text", "text": str(exc)}], "isError": True}
        raise McpError(f"fake server: unsupported method {method}")

    def _notify(self, method):
        return None


class StdioMcpClient(BaseMcpClient):
    """MCP over stdio: newline-delimited JSON-RPC 2.0 on a subprocess."""

    def __init__(self, command):
        super().__init__()
        if isinstance(command, str):
            command = shlex.split(command)
        self._proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        self._initialize()

    def _send(self, message):
        try:
            self._proc.stdin.write(json.dumps(message) + "\n")
            self._proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise McpError(f"stdio server closed: {exc}") from exc

    def _request(self, method, params):
        request_id = self._next_id()
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)
        while True:
            line = self._proc.stdout.readline()
            if not line:
                raise McpError(f"stdio server closed while awaiting {method}")
            try:
                response = json.loads(line)
            except ValueError:
                continue
            if response.get("id") != request_id:
                continue  # server-initiated message; ignore
            if "error" in response:
                raise McpError(f"{method}: {response['error'].get('message')}")
            return response.get("result")

    def _notify(self, method):
        self._send({"jsonrpc": "2.0", "method": method})

    def close(self):
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()


class HttpMcpClient(BaseMcpClient):
    """MCP over streamable HTTP (JSON responses, SSE tolerated)."""

    def __init__(self, url, headers=None, timeout=30, insecure=False):
        super().__init__()
        self._url = url
        self._headers = dict(headers or {})
        self._timeout = timeout
        if insecure:
            import ssl

            self._context = ssl._create_unverified_context()
        else:
            self._context = None
        self._session = None
        self._initialize()

    def _post(self, message):
        data = json.dumps(message).encode()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **self._headers,
        }
        if self._session:
            headers["Mcp-Session-Id"] = self._session
        request = urllib.request.Request(self._url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(request, timeout=self._timeout, context=self._context) as response:
            if self._session is None:
                session = response.headers.get("Mcp-Session-Id")
                if session:
                    self._session = session
            body = response.read().decode("utf-8", "replace")
            content_type = response.headers.get("Content-Type", "")
        return body, content_type

    def _request(self, method, params):
        request_id = self._next_id()
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        try:
            body, content_type = self._post(message)
        except OSError as exc:
            raise McpError(f"{method}: HTTP request failed: {exc}") from exc
        response = self._parse(body, content_type, request_id)
        if response is None:
            raise McpError(f"{method}: no response from {self._url}")
        if "error" in response:
            raise McpError(f"{method}: {response['error'].get('message')}")
        return response.get("result")

    def _notify(self, method):
        message = {"jsonrpc": "2.0", "method": method}
        try:
            self._post(message)
        except OSError:
            pass  # best effort for notifications

    @staticmethod
    def _parse(body, content_type, request_id):
        if "text/event-stream" in content_type:
            fallback = None
            for line in body.splitlines():
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if not payload:
                    continue
                try:
                    message = json.loads(payload)
                except ValueError:
                    continue
                if message.get("id") == request_id:
                    return message
                if fallback is None and ("result" in message or "error" in message):
                    fallback = message
            return fallback
        if not body.strip():
            return None
        try:
            return json.loads(body)
        except ValueError:
            return None


def from_config(url=None, command=None, insecure=False):
    """Build a client from a URL or a command string; None if neither is set."""
    if url:
        return HttpMcpClient(url, insecure=insecure)
    if command:
        return StdioMcpClient(command)
    return None


def from_env(prefix, insecure=None):
    """Build a client from {PREFIX}_MCP_URL / {PREFIX}_MCP_COMMAND env vars."""
    if insecure is None:
        insecure = os.environ.get("MCP_INSECURE_TLS", "").lower() in ("1", "true", "yes")
    url = os.environ.get(f"{prefix}_MCP_URL", "").strip()
    command = os.environ.get(f"{prefix}_MCP_COMMAND", "").strip()
    return from_config(url=url or None, command=command or None, insecure=insecure)


def _mock_command(role):
    return [
        "python3",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_mcp_server.py"),
        "--role",
        role,
    ]


def self_check():
    import mock_mcp_server

    mock_mcp_server.reset()
    fake = FakeMcpClient(mock_mcp_server.linux_tools(), mock_mcp_server.linux_call)
    names = [tool["name"] for tool in fake.list_tools()]
    if "run_command" not in names:
        raise SystemExit(f"mcp client: fake tools/list missing run_command: {names}")
    status = fake.call_tool("run_command", {"command": "systemctl status aiops-demo.service"})
    if "failed (Result: exit-code)" not in status:
        raise SystemExit("mcp client: fake tools/call returned unexpected status output")
    try:
        fake.call_tool("nope", {})
        raise SystemExit("mcp client: unknown tool should raise")
    except McpError:
        pass

    # Tool calls record name, arguments and output for the MLflow trace.
    import contextlib
    from unittest.mock import patch

    recorded = []

    @contextlib.contextmanager
    def fake_tool_span(tool, arguments):
        record = {"tool": tool, "arguments": arguments}
        recorded.append(record)
        yield record

    with patch.object(agent_tracing, "tool_span", fake_tool_span):
        status = fake.call_tool("run_command", {"command": "systemctl status aiops-demo.service"})
    if (recorded[-1]["tool"] != "run_command" or recorded[-1]["status"] != "ok"
            or "failed (Result: exit-code)" not in recorded[-1]["output"]):
        raise SystemExit(f"mcp client: tool call not recorded for tracing: {recorded[-1]}")
    with patch.object(agent_tracing, "tool_span", fake_tool_span):
        try:
            fake.call_tool("nope", {})
        except McpError:
            pass
    if recorded[-1]["status"] != "failed" or "unknown linux tool" not in recorded[-1]["output"]:
        raise SystemExit(f"mcp client: failed tool call not recorded: {recorded[-1]}")
    fake.close()

    # Exercise the real stdio JSON-RPC framing against the mock server.
    linux = StdioMcpClient(_mock_command("linux"))
    names = [tool["name"] for tool in linux.list_tools()]
    if "run_command" not in names:
        raise SystemExit(f"mcp client: stdio tools/list missing run_command: {names}")
    journal = linux.call_tool("run_command", {"command": "journalctl -u aiops-demo.service"})
    if "code=killed, status=9/KILL" not in journal:
        raise SystemExit("mcp client: stdio tools/call journal looks wrong")
    linux.close()

    ansible = StdioMcpClient(_mock_command("ansible"))
    modules = ansible.call_tool("list_approved_modules", {})
    if "ansible.builtin.systemd" not in modules:
        raise SystemExit("mcp client: stdio ansible approved modules missing systemd")
    ansible.close()
    print("mcp client: self-check ok", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Minimal MCP client")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument(
        "--probe",
        help="initialize against an MCP endpoint (URL or 'command string') and list tools",
    )
    args = parser.parse_args()
    if args.probe:
        insecure = os.environ.get("MCP_INSECURE_TLS", "").lower() in ("1", "true", "yes")
        if args.probe.startswith("http://") or args.probe.startswith("https://"):
            client = from_config(url=args.probe, insecure=insecure)
        else:
            client = from_config(command=args.probe)
        print(f"server: {client.server_info}", flush=True)
        for tool in client.list_tools():
            print(f"tool: {tool['name']}: {tool.get('description', '')}", flush=True)
        client.close()
        return
    if not args.self_check:
        raise SystemExit("pass --self-check or --probe '<url|command>'")
    self_check()


if __name__ == "__main__":
    main()
