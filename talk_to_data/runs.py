"""Run artifact persistence utilities."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any
from datetime import datetime, timezone

import pandas as pd


def create_run_dir(base_dir: Path) -> Path:
    """Create and return a timestamped run directory."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    run_dir = base_dir / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def sanitize_request(text: str) -> str:
    """Sanitize request text for logs."""
    single_line = re.sub(r"\s+", " ", text).strip()
    return single_line[:5000]


def save_run_artifacts(
    run_dir: Path,
    *,
    user_request: str,
    requirements: dict[str, Any],
    metadata_used: dict[str, Any],
    sql_candidates: list[dict[str, Any]],
    agent_info: dict[str, Any] | None = None,
    judge_result: dict[str, Any] | None = None,
) -> None:
    """Persist core run artifacts."""
    _save_json(run_dir / "requirements.json", requirements)
    _save_json(run_dir / "metadata_used.json", metadata_used)
    _save_json(run_dir / "sql_candidates.json", sql_candidates)
    if agent_info is not None:
        _save_json(run_dir / "agent_info.json", agent_info)
    if judge_result is not None:
        _save_json(run_dir / "judge_result.json", judge_result)
    (run_dir / "request.txt").write_text(sanitize_request(user_request), encoding="utf-8")


def save_result_excel(df: pd.DataFrame, run_dir: Path) -> Path:
    """Save query result to Excel and return file path."""
    output_path = run_dir / "result.xlsx"
    df.to_excel(output_path, index=False)
    return output_path


def save_result_preview(df: pd.DataFrame, run_dir: Path, max_rows: int = 200) -> Path:
    """Save a CSV preview for debugging and audit."""
    preview_path = run_dir / "result_preview.csv"
    df.head(max_rows).to_csv(preview_path, index=False)
    return preview_path


def save_result_interpretation(
    run_dir: Path,
    interpretation: dict[str, Any],
) -> Path:
    """Persist interpreted result summary and chart plan."""
    output_path = run_dir / "result_interpretation.json"
    _save_json(output_path, interpretation)
    return output_path


def save_llm_usage(run_dir: Path, usage: dict[str, Any]) -> Path:
    """Persist request-scoped LLM usage summary."""
    output_path = run_dir / "llm_usage.json"
    _save_json(output_path, usage)
    return output_path


def save_json_artifact(run_dir: Path, filename: str, payload: Any) -> Path:
    """Persist additional JSON artifact under run directory."""
    safe_name = str(filename).replace("\\", "/").split("/")[-1].strip()
    if not safe_name:
        raise ValueError("filename must not be empty.")
    if not safe_name.lower().endswith(".json"):
        raise ValueError("filename must end with .json.")
    output_path = run_dir / safe_name
    _save_json(output_path, payload)
    return output_path


def _save_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
