"""Requirement extraction using Prompt Part 2.5 style instructions."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError


class RequirementsExtractionError(RuntimeError):
    """Raised when structured requirement extraction fails."""


REQUIRED_KEYS = (
    "intent",
    "required_filters",
    "measures",
    "dimensions",
    "grain",
    "time_range",
    "report_period",
    "time_granularity",
    "time_value",
    "join_needs",
    "row_limit",
    "security_constraints",
)


PART_25_SYSTEM_PROMPT = (
    "You are a senior data modeler. "
    "Extract structured query requirements from a user request. "
    "Return strict JSON only with keys: "
    "intent, required_filters, measures, dimensions, grain, time_range, "
    "report_period, time_granularity, time_value, join_needs, row_limit, "
    "security_constraints, invalid_request, notes. "
    "Do not rewrite the user request; interpret it as-is."
)


def extract_requirements(
    user_request: str,
    llm_client: LLMClient | None = None,
    metadata_overview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Extract structured requirements from natural language request.

    Contract target: extract_requirements(user_request: str) -> dict
    """
    request = user_request.strip()
    if not request:
        raise RequirementsExtractionError("Request is empty.")

    if llm_client is None:
        return _heuristic_requirements(request, metadata_overview)

    try:
        raw = _run_extraction_prompt(
            system_prompt=PART_25_SYSTEM_PROMPT,
            prompt=user_request,
            llm_client=llm_client,
            temperature=0.0,
            max_tokens=1200,
        )
    except (LLMError, RuntimeError) as exc:
        return _heuristic_requirements(
            request,
            metadata_overview,
            warning=f"LLM unavailable, heuristic extraction used: {exc}",
        )

    if raw.strip().upper() == "INVALID_REQUEST":
        return _normalize_requirements(
            {"invalid_request": True, "intent": "unknown"},
        )

    parsed = _try_parse_json(raw)
    if parsed is None:
        raise RequirementsExtractionError(
            "Could not parse requirement JSON from model output."
        )

    return _normalize_requirements(parsed)


def _run_extraction_prompt(
    *,
    system_prompt: str,
    prompt: str,
    llm_client: LLMClient,
    temperature: float,
    max_tokens: int,
) -> str:
    return llm_client.chat(
        system_prompt,
        prompt,
        temperature=temperature,
        max_tokens=max_tokens,
    )


def _try_parse_json(raw: str) -> dict[str, Any] | None:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _normalize_requirements(data: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    normalized["intent"] = str(data.get("intent", "listing")).strip() or "listing"
    normalized["required_filters"] = _normalize_filter_list(data.get("required_filters"))
    normalized["measures"] = _as_string_list(data.get("measures"))
    normalized["dimensions"] = _as_string_list(data.get("dimensions"))
    normalized["grain"] = _as_string_list(data.get("grain"))
    normalized["join_needs"] = _as_string_list(data.get("join_needs"))
    normalized["security_constraints"] = _as_string_list(
        data.get("security_constraints")
    )
    normalized["invalid_request"] = bool(data.get("invalid_request", False))
    normalized["notes"] = str(data.get("notes", "")).strip()

    row_limit = _safe_int(data.get("row_limit"), fallback=200)
    normalized["row_limit"] = max(1, min(row_limit, 5000))

    normalized["report_period"] = _normalize_optional_text(data.get("report_period"))
    normalized["time_granularity"] = _normalize_optional_text(
        data.get("time_granularity")
    )
    normalized["time_value"] = _normalize_optional_text(data.get("time_value"))

    time_range = data.get("time_range")
    if isinstance(time_range, dict):
        normalized["time_range"] = {
            "start": _normalize_optional_text(time_range.get("start")),
            "end": _normalize_optional_text(time_range.get("end")),
        }
    else:
        normalized["time_range"] = {"start": None, "end": None}

    for key in REQUIRED_KEYS:
        normalized.setdefault(key, None)

    return normalized


def _heuristic_requirements(
    user_request: str,
    metadata_overview: dict[str, Any] | None,
    warning: str | None = None,
) -> dict[str, Any]:
    request_lower = user_request.lower()
    intent = "listing"
    if any(token in request_lower for token in ("total", "sum", "ortalama", "average")):
        intent = "metric"
    if any(token in request_lower for token in ("karsilastir", "compare", "difference")):
        intent = "comparison"
    if any(token in request_lower for token in ("anomaly", "aykiri", "outlier")):
        intent = "anomaly"

    row_limit = _extract_row_limit(user_request) or 200
    dimensions = _extract_named_tokens(user_request, prefixes=("by ", "gore "))
    measures = _extract_measure_candidates(user_request)

    required_filters: list[str] = []
    overview_filters = _as_string_list((metadata_overview or {}).get("mandatory_filters"))
    for mandatory in overview_filters:
        normalized = re.sub(r"\s+", " ", mandatory).strip()
        if normalized and normalized not in required_filters:
            required_filters.append(normalized)

    data = {
        "intent": intent,
        "required_filters": required_filters,
        "measures": measures,
        "dimensions": dimensions,
        "grain": [],
        "time_range": {"start": None, "end": None},
        "report_period": None,
        "time_granularity": None,
        "time_value": None,
        "join_needs": [],
        "row_limit": row_limit,
        "security_constraints": ["PII_DISALLOWED"],
        "invalid_request": False,
        "notes": warning or "Heuristic extraction was used.",
    }
    return _normalize_requirements(data)


def _extract_measure_candidates(user_request: str) -> list[str]:
    tokens = re.findall(r"\b[A-Z][A-Z0-9_]{2,}\b", user_request)
    return _dedupe(tokens[:8])


def _extract_named_tokens(user_request: str, prefixes: tuple[str, ...]) -> list[str]:
    lower = user_request.lower()
    values: list[str] = []
    for prefix in prefixes:
        idx = lower.find(prefix)
        if idx < 0:
            continue
        snippet = user_request[idx + len(prefix) : idx + len(prefix) + 60]
        words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", snippet)
        values.extend(word.upper() for word in words[:2])
    return _dedupe(values)


def _extract_row_limit(user_request: str) -> int | None:
    match = re.search(r"\b(top|first|ilk)\s+(\d{1,4})\b", user_request.lower())
    if not match:
        return None
    try:
        return int(match.group(2))
    except ValueError:
        return None


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        output: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                output.append(text)
        return _dedupe(output)
    return [str(value).strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(value)
    return unique


def _normalize_filter_list(value: Any) -> list[str]:
    normalized: list[str] = []
    for text in _as_string_list(value):
        expr = re.sub(r"\s+", " ", str(text)).strip()
        if expr and expr not in normalized:
            normalized.append(expr)
    return normalized


def _normalize_optional_text(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return text
