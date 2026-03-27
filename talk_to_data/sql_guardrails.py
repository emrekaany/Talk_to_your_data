"""Execution-time SQL guardrails."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError, compact_json
from .sql_generator import sanity_check_sql
from .sql_validation import analyze_sql_column_validation


class SQLGuardrailError(RuntimeError):
    """Raised when SQL fails execution-time guardrails."""


def validate_sql_before_execution(
    sql: str,
    metadata_used: dict[str, Any],
    llm_client: LLMClient | None = None,
    validation_catalog: dict[str, Any] | None = None,
) -> None:
    """Validate SQL safety and metadata allowlist compatibility."""
    del llm_client
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

    column_validation = analyze_sql_column_validation(
        sql=sql,
        metadata_used=metadata_used,
        validation_catalog=validation_catalog,
    )

    if column_validation.ambiguous_table_references:
        details = "; ".join(
            (
                f"{violation.scoped_reference} "
                f"(candidate metadata tables: {', '.join(violation.candidate_tables)})"
            )
            for violation in column_validation.ambiguous_table_references
        )
        raise SQLGuardrailError(
            "Column allowlist validation failed. "
            f"Ambiguous table references: {details}"
        )

    if column_validation.unknown_columns:
        details = "; ".join(
            (
                f"{violation.reference} "
                f"(expected metadata table: {violation.expected_table})"
            )
            for violation in column_validation.unknown_columns
        )
        raise SQLGuardrailError(
            "Column allowlist validation failed. "
            f"Unknown alias.column references: {details}"
        )

    if column_validation.unresolved_table_references:
        details = "; ".join(
            (
                f"{violation.reference} "
                f"(table {violation.known_table} exists in metadata but is not joined in the query)"
            )
            for violation in column_validation.unresolved_table_references
        )
        raise SQLGuardrailError(
            "Column allowlist validation failed. "
            f"Unresolved table references: {details}"
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
    global_obligations.extend(_as_string_list(metadata_used.get("runtime_mandatory_rules")))
    if global_obligations:
        obligation_map["__global__"] = _dedupe(global_obligations)
    return obligation_map


def _obligations_from_performance_rules(rules: list[str]) -> list[str]:
    del rules
    return []


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
    if _matches_granular_time_obligation(sql, obligation):
        return True

    normalized_sql = _normalize_space(sql).lower()
    normalized_obligation = _normalize_space(obligation).lower()

    if normalized_obligation and normalized_obligation in normalized_sql:
        return True

    if _is_granular_time_obligation(obligation):
        return False

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


def _is_granular_time_obligation(obligation: str) -> bool:
    lowered = _normalize_space(obligation).lower()
    if ":year_value" in lowered or ":date_value" in lowered:
        return True
    return ":report_period" in lowered and any(
        token in lowered for token in ("to_char", "substr", "regexp_replace")
    )


def _matches_granular_time_obligation(sql: str, obligation: str) -> bool:
    lowered = _normalize_space(obligation).lower()
    if ":year_value" in lowered:
        return _matches_to_char_bind(
            sql,
            "yyyy",
            "year_value",
        ) or _matches_numeric_date_like_bind(sql, "year_value", 4)
    if ":date_value" in lowered:
        return _matches_to_char_bind(
            sql,
            "yyyymmdd",
            "date_value",
        ) or _matches_numeric_date_like_bind(sql, "date_value", 8)
    if ":report_period" in lowered and "to_char" in lowered:
        return _matches_to_char_bind(
            sql,
            "yyyymm",
            "report_period",
        ) or _matches_numeric_date_like_bind(sql, "report_period", 6)
    if ":report_period" in lowered and any(
        token in lowered for token in ("substr", "regexp_replace")
    ):
        return _matches_numeric_date_like_bind(sql, "report_period", 6)
    return False


def _matches_to_char_bind(sql: str, format_mask: str, bind_name: str) -> bool:
    expr_variants = (
        r"[^,()]+(?:\s*\([^)]*\))?",
        r"trunc\s*\(\s*[^)]*?\s*\)",
        r"cast\s*\(\s*[^)]*?\s+as\s+(?:date|timestamp(?:\s*\(\d+\))?)\s*\)",
        r"trunc\s*\(\s*cast\s*\(\s*[^)]*?\s+as\s+(?:date|timestamp(?:\s*\(\d+\))?)\s*\)\s*\)",
    )
    for expr_pattern in expr_variants:
        to_char_pattern = (
            rf"to_char\s*\(\s*{expr_pattern}\s*,\s*['\"]{re.escape(format_mask)}['\"]\s*\)"
        )
        left_pattern = rf"{to_char_pattern}\s*=\s*:{re.escape(bind_name)}\b"
        right_pattern = rf":{re.escape(bind_name)}\b\s*=\s*{to_char_pattern}"
        if re.search(left_pattern, sql, flags=re.IGNORECASE) or re.search(
            right_pattern,
            sql,
            flags=re.IGNORECASE,
        ):
            return True
    return False


def _matches_numeric_date_like_bind(
    sql: str,
    bind_name: str,
    expected_length: int,
) -> bool:
    digit_expr = (
        r"substr\s*\(\s*regexp_replace\s*\(\s*(?:trim\s*\(\s*)?to_char\s*\(\s*[^)]+?\s*\)\s*\)?\s*,\s*'[^']+'\s*,\s*''\s*\)\s*,\s*1\s*,\s*"
        + str(expected_length)
        + r"\s*\)"
    )
    left_pattern = rf"{digit_expr}\s*=\s*:{re.escape(bind_name)}\b"
    right_pattern = rf":{re.escape(bind_name)}\b\s*=\s*{digit_expr}"
    return bool(
        re.search(left_pattern, sql, flags=re.IGNORECASE)
        or re.search(right_pattern, sql, flags=re.IGNORECASE)
    )


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
        ).content
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
