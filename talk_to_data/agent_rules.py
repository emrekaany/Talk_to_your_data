"""Agent-specific SQL prompt rule loading."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class AgentRulesError(RuntimeError):
    """Raised when agent SQL rule files are missing or malformed."""


def load_agent_rules(rules_path: Path, *, expected_agent_id: str) -> dict[str, Any]:
    """Load and validate one agent rule JSON file."""
    if not rules_path.exists():
        raise AgentRulesError(f"Missing agent rules file at '{rules_path}'.")

    try:
        payload = json.loads(rules_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AgentRulesError(f"Agent rules file is not valid JSON: '{rules_path}'.") from exc

    if not isinstance(payload, dict):
        raise AgentRulesError(f"Agent rules root must be an object: '{rules_path}'.")

    agent_id = str(payload.get("agent_id", "")).strip()
    if not agent_id:
        raise AgentRulesError(f"Agent rules missing required field 'agent_id': '{rules_path}'.")
    if agent_id != expected_agent_id:
        raise AgentRulesError(
            "Agent rules file agent_id mismatch: "
            f"expected '{expected_agent_id}', got '{agent_id}' at '{rules_path}'."
        )

    sql_prompt_rules = _normalize_string_list(payload.get("sql_prompt_rules"))
    time_expression_guidance = _normalize_string_list(payload.get("time_expression_guidance"))
    if payload.get("sql_prompt_rules") is None:
        raise AgentRulesError(
            f"Agent rules missing required field 'sql_prompt_rules': '{rules_path}'."
        )
    if payload.get("time_expression_guidance") is None:
        raise AgentRulesError(
            f"Agent rules missing required field 'time_expression_guidance': '{rules_path}'."
        )

    return {
        "agent_id": agent_id,
        "sql_prompt_rules": sql_prompt_rules,
        "time_expression_guidance": time_expression_guidance,
    }


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if not text:
            continue
        if text not in out:
            out.append(text)
    return out

