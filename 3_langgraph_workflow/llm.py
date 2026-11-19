#!/usr/bin/env python3
"""OpenAI-compatible chat client for the workflow agents.

Works with any /v1/chat/completions endpoint: vLLM on a GPU node, EnMaaS,
OpenAI, and so on. Configure with:

    LLM_BASE_URL   e.g. https://enmaas.example.com/v1 (include /v1)
    LLM_API_KEY    bearer token (optional for local vLLM)
    LLM_MODEL      model name served at the endpoint

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
import urllib.error
import urllib.request

STEP = "llm client"


class LlmError(Exception):
    pass


def config():
    return {
        "base_url": os.environ.get("LLM_BASE_URL", "").strip().rstrip("/"),
        "api_key": os.environ.get("LLM_API_KEY", "").strip(),
        "model": os.environ.get("LLM_MODEL", "").strip(),
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


def chat(messages, tools=None, temperature=0.0, max_tokens=1024):
    """One chat completion. Returns {"content": str, "tool_calls": list}."""
    if not available():
        raise LlmError("LLM not configured (set LLM_BASE_URL and LLM_MODEL)")
    request = build_request(messages, tools=tools, temperature=temperature, max_tokens=max_tokens)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        raise LlmError(f"HTTP {exc.code}: {exc.read().decode('utf-8', 'replace')[:200]}") from exc
    except OSError as exc:
        raise LlmError(f"request failed: {exc}") from exc
    try:
        choice = body["choices"][0]["message"]
    except (KeyError, IndexError) as exc:
        raise LlmError(f"unexpected response shape: {json.dumps(body)[:200]}") from exc
    return {
        "content": choice.get("content") or "",
        "tool_calls": choice.get("tool_calls") or [],
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
    raise LlmError(f"no JSON object in reply: {text[:200]}")


def chat_json(messages, temperature=0.0, max_tokens=1024):
    """Chat completion parsed as a JSON object (agents use JSON decisions)."""
    reply = chat(messages, temperature=temperature, max_tokens=max_tokens)
    return _extract_json(reply["content"])


def self_check():
    import os

    saved = {key: os.environ.get(key) for key in ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL")}
    os.environ["LLM_BASE_URL"] = "https://llm.example.com/v1"
    os.environ["LLM_MODEL"] = "test-model"
    os.environ.pop("LLM_API_KEY", None)
    try:
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
            raise SystemExit("LLM not configured (set LLM_BASE_URL and LLM_MODEL)")
        reply = chat([{"role": "user", "content": "Reply with the single word: ok"}])
        print(f"model replied: {reply['content'][:200]}", flush=True)
        return
    if not args.self_check:
        raise SystemExit("pass --self-check or --probe")
    self_check()


if __name__ == "__main__":
    main()
