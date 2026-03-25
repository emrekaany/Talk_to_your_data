"""Minimal chat-completions client used across the app."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any
import urllib.error
import urllib.request

from .llm_logging import log_prompt, log_response


class LLMError(RuntimeError):
    """Raised when LLM invocation fails."""


@dataclass
class LLMResponse:
    """Structured response from a chat-completions call."""

    content: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    duration_sec: float = 0.0

    def __str__(self) -> str:
        return self.content


@dataclass
class LLMClient:
    """Small wrapper around an OpenAI-compatible chat-completions endpoint."""

    api_key: str
    url: str
    model: str
    timeout_sec: int = 60

    def chat(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        temperature: float = 0.2,
        max_tokens: int = 1200,
    ) -> LLMResponse:
        """Send a chat-completions request and return structured response."""
        if not self.api_key:
            raise LLMError("LLM_API_KEY is not set.")

        log_prompt(
            source="talk_to_data.llm_client",
            model=self.model,
            url=self.url,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
        )

        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise LLMError(
                f"LLM HTTP error {exc.code}: {body[:300] or exc.reason}"
            ) from exc
        except urllib.error.URLError as exc:
            raise LLMError(f"LLM connection error: {exc.reason}") from exc
        duration = time.perf_counter() - t0

        content, usage = _extract_content_and_usage(raw)
        resp = LLMResponse(
            content=content,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            duration_sec=round(duration, 3),
        )
        log_response(
            source="talk_to_data.llm_client",
            duration_sec=resp.duration_sec,
            prompt_tokens=resp.prompt_tokens,
            completion_tokens=resp.completion_tokens,
            total_tokens=resp.total_tokens,
        )
        return resp


def _extract_content_and_usage(raw: str) -> tuple[str, dict[str, int]]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip(), {}

    if not isinstance(data, dict):
        return raw.strip(), {}

    usage: dict[str, int] = {}
    raw_usage = data.get("usage")
    if isinstance(raw_usage, dict):
        for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
            val = raw_usage.get(key)
            if isinstance(val, (int, float)):
                usage[key] = int(val)

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip(), usage
            text = first.get("text")
            if isinstance(text, str):
                return text.strip(), usage

    return raw.strip(), usage


def try_build_llm_client(
    *,
    api_key: str,
    url: str,
    model: str,
    timeout_sec: int,
) -> LLMClient | None:
    """Create LLM client only when credentials are present."""
    if not api_key:
        return None
    return LLMClient(api_key=api_key, url=url, model=model, timeout_sec=timeout_sec)


def compact_json(data: Any) -> str:
    """Render compact JSON text for prompts."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
