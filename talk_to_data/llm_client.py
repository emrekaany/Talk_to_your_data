"""Minimal chat-completions client used across the app."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
import urllib.error
import urllib.request

from .llm_logging import log_prompt


class LLMError(RuntimeError):
    """Raised when LLM invocation fails."""


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
    ) -> str:
        """Send a chat-completions request and return plain text content."""
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

        return _extract_content(raw)


def _extract_content(raw: str) -> str:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip()

    if not isinstance(data, dict):
        return raw.strip()

    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            message = first.get("message")
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip()
            text = first.get("text")
            if isinstance(text, str):
                return text.strip()

    return raw.strip()


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
