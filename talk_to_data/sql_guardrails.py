"""Execution-time SQL guardrails."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError, compact_json
from .sql_generator import sanity_check_sql


class SQLGuardrailError(RuntimeError):
    """Raised when SQL fails execution-time guardrails."""


def validate_sql_before_execution(
    sql: str,
    metadata_used: dict[str, Any],
    llm_client: LLMClient | None = None,
) -> None:
    """Validate SQL safety, allowlisted tables, and filter obligations."""
    ok, reason = sanity_check_sql(sql)
    if not ok:
        raise SQLGuardrailError(f"Safety validation failed: {reason}")

    selected_tables = _extract_selected_tables(sql)
    allowlist = _build_allowlist(metadata_used)
    cte_names = _extract_cte_names(sql)

    disallowed: list[str] = []
    for table in selected_tables:
        if table in cte_names:
            continue
        if not _is_allowed_table(table, allowlist):
            disallowed.append(table)
    if disallowed:
        raise SQLGuardrailError(
            "Table allowlist validation failed. "
            f"Disallowed tables: {', '.join(disallowed)}"
        )

    obligation_map = _build_table_obligations(metadata_used, selected_tables)
    missing = _find_missing_obligations(sql, obligation_map)
    if missing and llm_client is not None:
        checked = _llm_check_obligations(
            sql=sql,
            selected_tables=selected_tables,
            obligation_map=obligation_map,
            llm_client=llm_client,
        )
        if checked is not None:
            missing = checked

    if missing:
        raise SQLGuardrailError(
            "Filter obligation validation failed. Missing obligations: "
            f"{'; '.join(missing)}"
        )


def _extract_selected_tables(sql: str) -> list[str]:
    matches = re.findall(
        r"\b(?:from|join)\s+([A-Za-z0-9_.$#\"]+)",
        sql,
        flags=re.IGNORECASE,
    )
    tables: list[str] = []
    for token in matches:
        normalized = _normalize_identifier(token)
        if normalized:
            tables.append(normalized)
    return _dedupe(tables)


def _extract_cte_names(sql: str) -> set[str]:
    names = re.findall(
        r"(?:\bwith\b|,)\s*([A-Za-z_][A-Za-z0-9_$#]*)\s+as\s*\(",
        sql,
        flags=re.IGNORECASE,
    )
    return {name.strip().lower() for name in names if name.strip()}


def _build_allowlist(metadata_used: dict[str, Any]) -> set[str]:
    allowlist: set[str] = set()
    items = metadata_used.get("relevant_items")
    if not isinstance(items, list):
        return allowlist

    for item in items:
        if not isinstance(item, dict):
            continue
        table = _normalize_identifier(str(item.get("table", "")))
        if not table:
            continue
        allowlist.add(table)
        bare = table.split(".")[-1]
        allowlist.add(bare)
    return allowlist


def _is_allowed_table(table: str, allowlist: set[str]) -> bool:
    normalized = _normalize_identifier(table)
    if not allowlist:
        return True
    if normalized in allowlist:
        return True
    bare = normalized.split(".")[-1]
    return bare in allowlist


def _build_table_obligations(
    metadata_used: dict[str, Any],
    selected_tables: list[str],
) -> dict[str, list[str]]:
    obligation_map: dict[str, list[str]] = {}
    selected_set = {_normalize_identifier(table) for table in selected_tables}
    selected_bare = {table.split(".")[-1] for table in selected_set}

    items = metadata_used.get("relevant_items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            table = _normalize_identifier(str(item.get("table", "")))
            if not table:
                continue
            table_bare = table.split(".")[-1]
            if table not in selected_set and table_bare not in selected_bare:
                continue

            obligations: list[str] = []
            obligations.extend(_as_string_list(item.get("mandatory_filters")))
            obligations.extend(
                _obligations_from_performance_rules(
                    _as_string_list(item.get("performance_rules"))
                )
            )
            if obligations:
                obligation_map[table] = _dedupe(obligations)

    global_obligations = _as_string_list(metadata_used.get("mandatory_rules"))
    if global_obligations:
        obligation_map["__global__"] = _dedupe(global_obligations)
    return obligation_map


def _obligations_from_performance_rules(rules: list[str]) -> list[str]:
    obligations: list[str] = []
    for rule in rules:
        low = rule.lower()
        if "report_period" in low and any(
            token in low for token in ("mandatory", "required", "without", "zorunlu")
        ):
            obligations.append("REPORT_PERIOD = :report_period")
    return obligations


def _find_missing_obligations(
    sql: str,
    obligation_map: dict[str, list[str]],
) -> list[str]:
    missing: list[str] = []
    for table, obligations in obligation_map.items():
        for obligation in obligations:
            if not _obligation_satisfied(sql, obligation):
                label = "GLOBAL" if table == "__global__" else table
                missing.append(f"{label}: {obligation}")
    return _dedupe(missing)


def _obligation_satisfied(sql: str, obligation: str) -> bool:
    normalized_sql = _normalize_space(sql).lower()
    normalized_obligation = _normalize_space(obligation).lower()

    if normalized_obligation and normalized_obligation in normalized_sql:
        return True

    column = _extract_column_token(obligation)
    if column:
        where_match = re.search(
            r"\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\bhaving\b|\bfetch\s+first\b|$)",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if where_match and re.search(
            rf"\b{re.escape(column)}\b",
            where_match.group(1),
            flags=re.IGNORECASE,
        ):
            return True
    return False


def _extract_column_token(obligation: str) -> str | None:
    match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", obligation)
    if not match:
        return None
    return match.group(1)


def _llm_check_obligations(
    *,
    sql: str,
    selected_tables: list[str],
    obligation_map: dict[str, list[str]],
    llm_client: LLMClient,
) -> list[str] | None:
    prompt = (
        "Check whether SQL satisfies required filter obligations.\n"
        "Return strict JSON only in this schema:\n"
        '{"missing_obligations":["TABLE: OBLIGATION"]}\n'
        "If all obligations are satisfied, return an empty list.\n\n"
        f"Selected tables: {compact_json(selected_tables)}\n"
        f"Obligations: {compact_json(obligation_map)}\n"
        f"SQL:\n{sql}"
    )
    try:
        raw = llm_client.chat(
            "You are a strict SQL guardrail validator.",
            prompt,
            temperature=0.0,
            max_tokens=500,
        )
    except LLMError:
        return None

    parsed = _parse_json(raw)
    if not isinstance(parsed, dict):
        return None
    values = parsed.get("missing_obligations")
    if not isinstance(values, list):
        return None
    return _dedupe([str(value).strip() for value in values if str(value).strip()])


def _parse_json(raw: str) -> dict[str, Any] | None:
    stripped = raw.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _normalize_identifier(value: str) -> str:
    identifier = value.strip().strip(",")
    if identifier.startswith('"') and identifier.endswith('"'):
        identifier = identifier[1:-1]
    identifier = identifier.replace('"', "")
    return identifier.lower()


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    return [str(value).strip()]


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out
