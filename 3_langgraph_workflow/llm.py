#!/usr/bin/env python3
"""OpenAI-compatible chat client for the workflow agents.

Works with any /v1/chat/completions endpoint: vLLM on a GPU node, EnMaaS,
OpenAI, and so on. Configure with:

    LLM_BASE_URL   e.g. https://enmaas.example.com/v1 (include /v1)
    LLM_API_KEY    bearer token (optional for local vLLM)
    LLM_MODEL      optional override for the default Muse Spark model

Every agent falls back to deterministic heuristics when the LLM is not
configured or a call fails, so the demo always runs.

Verify (no network needed):

    python3 3_langgraph_workflow/llm.py --self-check

Probe a live endpoint:

    python3 3_langgraph_workflow/llm.py --probe
"""

import argparse
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
import agent_tracing
from state import ist_timestamp

STEP = "llm client"
DEFAULT_MODEL = "rits/meta-models/Muse-Spark-Open-816B-A42B-NVFP4"
VM_CALL_INTERVAL = 10
CALL_START_GAP = 5


class CallPacer:
    """Reserve start times against the actual clock; never lock during network I/O."""
    def __init__(self, clock=time.monotonic, sleep=time.sleep):
        self.clock, self.sleep = clock, sleep
        self.lock = threading.Lock()
        self.next_global = 0
        self.next_vm = {}

    def wait(self, vm):
        with self.lock:
            due = max(self.next_global, self.next_vm.get(vm, 0))
            while (delay := due - self.clock()) > 0:
                self.sleep(delay)
            start = self.clock()
            self.next_global = start + CALL_START_GAP
            self.next_vm[vm] = start + VM_CALL_INTERVAL


_pacer = CallPacer()


class LlmError(Exception):
    pass


class IncompleteReply(LlmError):
    """The model ran out of output tokens before returning usable JSON."""


def config():
    return {
        "base_url": os.environ.get("LLM_BASE_URL", "").strip().rstrip("/"),
        "api_key": os.environ.get("LLM_API_KEY", "").strip(),
        "model": os.environ.get("LLM_MODEL", "").strip() or DEFAULT_MODEL,
    }


def available():
    cfg = config()
    return bool(cfg["base_url"] and cfg["model"])


def build_request(messages, tools=None, temperature=0.0, max_tokens=1024):
    cfg = config()
    payload = {
        "model": cfg["model"],
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if tools:
        payload["tools"] = tools
    headers = {"Content-Type": "application/json"}
    if cfg["api_key"]:
        headers["Authorization"] = f"Bearer {cfg['api_key']}"
    url = cfg["base_url"] + "/chat/completions"
    return urllib.request.Request(
        url, data=json.dumps(payload).encode(), headers=headers, method="POST"
    )


def _report(service_name, phase, message):
    """One progress line per call state, so pod logs show where time goes."""
    if service_name is not None:
        print(f"[{ist_timestamp()}] [llm client] {service_name} {phase} {message}", flush=True)


def chat(messages, tools=None, temperature=0.0, max_tokens=1024, service_name=None, phase="call"):
    """One chat completion. Returns {"content": str, "tool_calls": list}."""
    if not available():
        raise LlmError("LLM not configured (set LLM_BASE_URL)")
    model = config()["model"]
    request = build_request(messages, tools=tools, temperature=temperature, max_tokens=max_tokens)
    if service_name is not None:
        _pacer.wait(service_name)
        _report(service_name, phase, f"model call starting ({model})")
    started = time.monotonic()
    with agent_tracing.llm_span(service_name, phase,
                                {"model": model, "max_tokens": max_tokens,
                                 "tools": len(tools or [])}) as record:
        record["status"] = "in-flight"
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = json.loads(response.read().decode("utf-8", "replace"))
            record["status"] = "ok"
        except urllib.error.HTTPError as exc:
            record["status"] = f"failed: HTTP {exc.code}"
            _report(service_name, phase,
                    f"model call failed after {time.monotonic() - started:.1f}s "
                    f"(endpoint returned HTTP {exc.code})")
            raise LlmError(f"model endpoint returned HTTP {exc.code}") from exc
        except OSError as exc:
            record["status"] = f"failed: {type(exc).__name__}"
            _report(service_name, phase,
                    f"model call failed after {time.monotonic() - started:.1f}s "
                    f"(connection failed: {type(exc).__name__})")
            raise LlmError(f"model connection failed ({type(exc).__name__})") from exc
        finally:
            record["duration_ms"] = round((time.monotonic() - started) * 1000)
        try:
            choice = body["choices"][0]["message"]
        except (KeyError, IndexError, TypeError) as exc:
            record["status"] = "failed: no chat message in reply"
            raise LlmError("model endpoint returned no chat message") from exc
        finish_reason = body["choices"][0].get("finish_reason")
        record["finish_reason"] = finish_reason
        usage = body.get("usage") or {}
        tokens = {key: usage[key] for key in
                  ("prompt_tokens", "completion_tokens", "total_tokens") if key in usage}
        if tokens:
            record["tokens"] = tokens
        _report(service_name, phase,
                f"model call finished in {time.monotonic() - started:.1f}s "
                f"(finish_reason={finish_reason})")
    return {
        "content": choice.get("content") or "",
        "tool_calls": choice.get("tool_calls") or [],
        "finish_reason": finish_reason,
    }


def _extract_json(text):
    """Parse a JSON object out of an LLM reply, tolerating code fences."""
    if not text:
        raise LlmError("empty LLM reply")
    stripped = text.strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", stripped, re.DOTALL)
    if fence:
        stripped = fence.group(1).strip()
    try:
        return json.loads(stripped)
    except ValueError:
        pass
    start, end = stripped.find("{"), stripped.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(stripped[start : end + 1])
        except ValueError:
            pass
    raise LlmError("model reply did not contain valid JSON")


def chat_json(messages, temperature=0.0, max_tokens=1024, service_name=None, phase="decision"):
    """Chat completion parsed as a JSON object (agents use JSON decisions)."""
    reply = chat(messages, temperature=temperature, max_tokens=max_tokens,
                 service_name=service_name, phase=phase)
    try:
        return _extract_json(reply["content"])
    except LlmError as exc:
        _report(service_name, phase,
                f"model reply was not usable JSON (finish_reason={reply.get('finish_reason')})")
        if reply.get("finish_reason") == "length":
            raise IncompleteReply(f"model reached the {max_tokens}-token output limit before returning JSON") from exc
        raise


def self_check():
    import os

    now = [0]
    pacer = CallPacer(lambda: now[0], lambda delay: now.__setitem__(0, now[0] + delay))
    for vm, expected in [("a", 0), ("b", 5), ("b", 15), ("c", 20), ("a", 25)]:
        pacer.wait(vm)
        assert now[0] == expected

    saved = {key: os.environ.get(key) for key in ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL")}
    os.environ["LLM_BASE_URL"] = "https://llm.example.com/v1"
    os.environ.pop("LLM_API_KEY", None)
    try:
        os.environ.pop("LLM_MODEL", None)
        assert config()["model"] == DEFAULT_MODEL and available()
        os.environ["LLM_MODEL"] = "test-model"
        parsed = _extract_json('Sure! ```json\n{"detected": true, "score": 2}\n```')
        if parsed != {"detected": True, "score": 2}:
            raise SystemExit(f"{STEP}: fenced JSON not parsed: {parsed}")
        parsed = _extract_json('The answer is {"a": 1} hope that helps')
        if parsed != {"a": 1}:
            raise SystemExit(f"{STEP}: embedded JSON not parsed: {parsed}")
        try:
            _extract_json("no json here")
            raise SystemExit(f"{STEP}: garbage should not parse")
        except LlmError:
            pass
        request = build_request(
            [{"role": "user", "content": "hi"}],
        )
        if request.full_url != "https://llm.example.com/v1/chat/completions":
            raise SystemExit(f"{STEP}: bad request url {request.full_url}")
        data = json.loads(request.data.decode())
        if data["messages"][0]["content"] != "hi" or data["temperature"] != 0.0:
            raise SystemExit(f"{STEP}: bad request payload {data}")
        if data["model"] != "test-model":
            raise SystemExit(f"{STEP}: bad model in payload {data}")
        if "Authorization" in request.headers:
            raise SystemExit(f"{STEP}: auth header without api key")
        os.environ["LLM_API_KEY"] = "secret"
        request = build_request([{"role": "user", "content": "hi"}])
        if request.headers.get("Authorization") != "Bearer secret":
            raise SystemExit(f"{STEP}: bearer token not set")

        # chat(): each call records its status for MLflow and pod logs.
        import contextlib
        import sys
        from unittest.mock import patch

        this = sys.modules[__name__]  # patch the running module, not a second import
        captured = []

        @contextlib.contextmanager
        def fake_llm_span(service, phase, inputs=None):
            record = {"service": service, "phase": phase, **(inputs or {})}
            captured.append(record)
            yield record

        reply_body = {
            "choices": [{"message": {"content": '{"ok": true}'}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11},
        }

        class FakeResponse:
            def __init__(self, body):
                self._body = body

            def read(self):
                return self._body

        def fake_urlopen(request, timeout=None):
            return contextlib.nullcontext(FakeResponse(json.dumps(reply_body).encode()))

        with patch.object(agent_tracing, "llm_span", fake_llm_span), \
                patch.object(urllib.request, "urlopen", fake_urlopen), \
                patch.object(this, "_pacer", pacer):
            reply = chat([{"role": "user", "content": "hi"}],
                         service_name="vm-9", phase="self-check")
        if reply["content"] != '{"ok": true}' or reply["finish_reason"] != "stop":
            raise SystemExit(f"{STEP}: chat reply wrong {reply}")
        record = captured[-1]
        if (record["phase"] != "self-check" or record["status"] != "ok"
                or "duration_ms" not in record or record["finish_reason"] != "stop"
                or record["tokens"]["total_tokens"] != 11):
            raise SystemExit(f"{STEP}: successful call status not recorded {record}")

        def refuse(request, timeout=None):
            raise urllib.error.HTTPError(request.full_url, 429, "too many requests", None, None)

        with patch.object(agent_tracing, "llm_span", fake_llm_span), \
                patch.object(urllib.request, "urlopen", refuse), \
                patch.object(this, "_pacer", pacer):
            try:
                chat([{"role": "user", "content": "hi"}],
                     service_name="vm-9", phase="self-check")
                raise SystemExit(f"{STEP}: HTTP failure should raise LlmError")
            except LlmError as exc:
                if "429" not in str(exc):
                    raise SystemExit(f"{STEP}: HTTP failure should surface the code: {exc}")
        record = captured[-1]
        if (not str(record["status"]).startswith("failed") or "duration_ms" not in record):
            raise SystemExit(f"{STEP}: failed call status not recorded {record}")

        with patch.object(this, "chat",
                          return_value={"content": '{"a": 1}', "finish_reason": "stop"}) as fake:
            parsed = chat_json([{"role": "user", "content": "x"}],
                               service_name="vm-9", phase="self-check")
        if parsed != {"a": 1} or fake.call_args.kwargs.get("phase") != "self-check":
            raise SystemExit(f"{STEP}: chat_json must pass phase through {fake.call_args}")
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    print(f"{STEP}: self-check ok (configured={available()})", flush=True)


def main():
    parser = argparse.ArgumentParser(description="OpenAI-compatible LLM client")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--probe", action="store_true", help="send one live chat request")
    args = parser.parse_args()
    if args.probe:
        if not available():
            raise SystemExit("LLM not configured (set LLM_BASE_URL)")
        reply = chat([{"role": "user", "content": "Reply with the single word: ok"}])
        print(f"model replied: {reply['content'][:200]}", flush=True)
        return
    if not args.self_check:
        raise SystemExit("pass --self-check or --probe")
    self_check()


if __name__ == "__main__":
    main()
