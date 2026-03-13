"""Helpers for persisting outgoing LLM prompts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import threading
from typing import Any


_LOG_LOCK = threading.Lock()


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

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(entry, ensure_ascii=False)
        with _LOG_LOCK:
            with path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")
    except OSError:
        return
