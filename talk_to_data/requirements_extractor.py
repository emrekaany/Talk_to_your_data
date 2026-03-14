"""Requirement extraction using Prompt Part 2.5 style instructions."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError, compact_json


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
    "join_needs",
    "row_limit",
    "security_constraints",
)


PART_25_SYSTEM_PROMPT = (
    "You are a senior data modeler. "
    "Extract structured query requirements from a user request. "
    "Output strict JSON only."
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

    prompt = _build_extraction_prompt(request, metadata_overview)
    try:
        raw = _run_extraction_prompt(
            system_prompt=PART_25_SYSTEM_PROMPT,
            prompt=prompt,
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
            metadata_overview=metadata_overview,
        )

    parsed = _try_parse_json(raw)
    if parsed is None:
        parsed = _retry_fix_json(raw, llm_client)
    if parsed is None:
        raise RequirementsExtractionError(
            "Could not parse requirement JSON from model output."
        )

    return _normalize_requirements(parsed, metadata_overview=metadata_overview)


def _build_extraction_prompt(
    user_request: str,
    metadata_overview: dict[str, Any] | None,
) -> str:
    schema_text = (
        '{"intent":"listing|metric|comparison|anomaly|other",'
        '"required_filters":["<optional filter expression>"],'
        '"measures":["PRIM_TL"],'
        '"dimensions":["BRANS_KODU"],'
        '"grain":["POLICE_NO"],'
        '"time_range":{"start":"YYYY-MM-DD","end":"YYYY-MM-DD"},'
        '"report_period":"YYYYMM",'
        '"join_needs":["AS_IFRS.TABLE_A -> AS_IFRS.TABLE_B"],'
        '"row_limit":200,'
        '"security_constraints":["PII_DISALLOWED"],'
        '"invalid_request":false,'
        '"notes":"optional"}'
    )
    overview = compact_json(metadata_overview or {})
    return (
        "Task: Read the request and return strict JSON with query requirements.\n"
        "Rules:\n"
        "- Output JSON only, no markdown.\n"
        "- Include all keys shown in schema.\n"
        "- Keep values concise.\n"
        "- Use Oracle-safe placeholders when relevant (:report_period).\n"
        "- For every mandatory filter from metadata overview, include an explicit filter predicate in required_filters.\n"
        "- If a mandatory filter is a column name (example REPORT_PERIOD), convert it to COLUMN = :column_bind.\n"
        "- If metadata overview has time_filter_policy=ek_tanzim_date, use ek tanzim date context for period filters and avoid REPORT_PERIOD column filters.\n"
        "- Do not invent REPORT_PERIOD filters unless metadata indicates REPORT_PERIOD exists or mandates it.\n"
        "- If request contains a concrete period (YYYYMM), fill report_period with that value.\n"
        "- If request cannot be answered, set invalid_request=true and provide reason in notes.\n\n"
        f"Schema:\n{schema_text}\n\n"
        f"Metadata overview:\n{overview}\n\n"
        f"Request:\n{user_request}"
    )


def _retry_fix_json(raw_text: str, llm_client: LLMClient | None) -> dict[str, Any] | None:
    fix_prompt = (
        "Fix the text below into strict valid JSON only. "
        "Do not add explanations. Preserve semantics.\n\n"
        f"Text:\n{raw_text}"
    )
    try:
        fixed = _run_extraction_prompt(
            system_prompt=PART_25_SYSTEM_PROMPT,
            prompt=fix_prompt,
            llm_client=llm_client,
            temperature=0.0,
            max_tokens=1200,
        )
    except (LLMError, RuntimeError):
        return None
    return _try_parse_json(fixed)


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


def _normalize_requirements(
    data: dict[str, Any],
    *,
    metadata_overview: dict[str, Any] | None = None,
) -> dict[str, Any]:
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

    report_period = data.get("report_period")
    normalized["report_period"] = (
        str(report_period).strip() if report_period not in (None, "", []) else None
    )

    time_range = data.get("time_range")
    if isinstance(time_range, dict):
        normalized["time_range"] = {
            "start": _normalize_optional_text(time_range.get("start")),
            "end": _normalize_optional_text(time_range.get("end")),
        }
    else:
        normalized["time_range"] = {"start": None, "end": None}

    _apply_report_period_policy(normalized, metadata_overview)

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

    report_period = _extract_report_period(user_request)
    row_limit = _extract_row_limit(user_request) or 200

    dimensions = _extract_named_tokens(user_request, prefixes=("by ", "gore "))
    measures = _extract_measure_candidates(user_request)

    required_filters: list[str] = []

    overview_filters = _as_string_list((metadata_overview or {}).get("mandatory_filters"))
    for mandatory in overview_filters:
        if mandatory not in required_filters:
            required_filters.append(mandatory)

    data = {
        "intent": intent,
        "required_filters": required_filters,
        "measures": measures,
        "dimensions": dimensions,
        "grain": [],
        "time_range": {"start": None, "end": None},
        "report_period": report_period,
        "join_needs": [],
        "row_limit": row_limit,
        "security_constraints": ["PII_DISALLOWED"],
        "invalid_request": False,
        "notes": warning or "Heuristic extraction was used.",
    }
    return _normalize_requirements(data, metadata_overview=metadata_overview)


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


def _extract_report_period(user_request: str) -> str | None:
    match = re.search(r"\b(20\d{2}(0[1-9]|1[0-2]))\b", user_request)
    if match:
        return match.group(1)
    return None


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
        expr = _normalize_filter_expression(text)
        if expr and expr not in normalized:
            normalized.append(expr)
    return normalized


def _normalize_filter_expression(filter_text: str) -> str:
    text = re.sub(r"\s+", " ", str(filter_text)).strip()
    if not text:
        return ""
    if re.search(r"(=|<>|!=|<=|>=|<|>|\blike\b|\bbetween\b|\bin\b|\bis\b)", text, flags=re.IGNORECASE):
        return text

    token_match = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text)
    if not token_match:
        return text

    column = token_match.group(0).upper()
    bind_name = "report_period" if column.lower() == "report_period" else column.lower()
    return f"{column} = :{bind_name}"


def _normalize_optional_text(value: Any) -> str | None:
    if value in (None, "", []):
        return None
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return text


def _apply_report_period_policy(
    normalized: dict[str, Any],
    metadata_overview: dict[str, Any] | None,
) -> None:
    report_period = str(normalized.get("report_period") or "").strip()
    if not report_period:
        return

    filters = normalized.get("required_filters")
    if not isinstance(filters, list):
        return

    overview = metadata_overview or {}
    policy = str(overview.get("time_filter_policy", "")).strip().lower()
    prefers_ek_tanzim = policy == "ek_tanzim_date"

    if prefers_ek_tanzim:
        normalized["required_filters"] = [
            flt for flt in filters if not _references_report_period_column(flt)
        ]
        return

    if _contains_report_period_reference(filters):
        return

    if _can_use_report_period_column(overview):
        normalized["required_filters"].append("REPORT_PERIOD = :report_period")


def _contains_report_period_reference(filters: list[str]) -> bool:
    for flt in filters:
        text = str(flt)
        if re.search(r":\s*report_period\b", text, flags=re.IGNORECASE):
            return True
        if _references_report_period_column(text):
            return True
    return False


def _references_report_period_column(filter_text: str) -> bool:
    return bool(
        re.search(
            r"(?<!:)\breport_period\b",
            str(filter_text),
            flags=re.IGNORECASE,
        )
    )


def _can_use_report_period_column(metadata_overview: dict[str, Any]) -> bool:
    if bool(metadata_overview.get("has_report_period_column")):
        return True
    mandatory = _as_string_list(metadata_overview.get("mandatory_filters"))
    return any(_references_report_period_column(flt) for flt in mandatory)
