"""Helpers for persisting outgoing LLM prompts."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
from typing import Any


_LOG_LOCK = threading.Lock()
_ACTIVE_CAPTURE: ContextVar["LLMCallCapture | None"] = ContextVar(
    "ACTIVE_LLM_CALL_CAPTURE",
    default=None,
)


@dataclass
class LLMCallCapture:
    """Mutable per-scope LLM call accounting."""

    label: str
    started_at_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    finished_at_utc: str | None = None
    total_calls: int = 0
    by_source: dict[str, int] = field(default_factory=dict)

    def record(self, source: str) -> None:
        self.total_calls += 1
        self.by_source[source] = self.by_source.get(source, 0) + 1

    def finalize(self) -> None:
        if self.finished_at_utc is None:
            self.finished_at_utc = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        self.finalize()
        return {
            "label": self.label,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "total_calls": self.total_calls,
            "by_source": dict(sorted(self.by_source.items())),
        }


@contextmanager
def capture_llm_calls(label: str) -> Any:
    """Capture prompt counts for a bounded workflow such as one request."""
    capture = LLMCallCapture(label=label)
    token = _ACTIVE_CAPTURE.set(capture)
    try:
        yield capture
    finally:
        capture.finalize()
        _ACTIVE_CAPTURE.reset(token)


def resolve_prompt_log_path() -> Path:
    """Resolve prompt log path from environment with sensible defaults."""
    raw_path = os.getenv("LLM_PROMPT_LOG_PATH", "").strip()
    if raw_path:
        return Path(raw_path)
    runs_dir = os.getenv("RUNS_DIR", "runs").strip() or "runs"
    return Path(runs_dir) / "llm_prompts.log"


def log_prompt(
    *,
    source: str,
    model: str,
    url: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_tokens: int,
    metadata: dict[str, Any] | None = None,
) -> None:
    """
    Append one prompt entry as JSON line.

    Logging failures are intentionally non-blocking.
    """
    path = resolve_prompt_log_path()
    entry: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "model": model,
        "url": url,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }
    if metadata:
        entry["metadata"] = metadata

    capture = _ACTIVE_CAPTURE.get()
    if capture is not None:
        capture.record(source)

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(entry, ensure_ascii=False)
        with _LOG_LOCK:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")
    except OSError:
        return
