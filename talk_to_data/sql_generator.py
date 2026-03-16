"""SQL candidate generation and safety checks."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError, compact_json


BLOCKED_KEYWORDS = (
    "drop",
    "delete",
    "insert",
    "update",
    "merge",
    "alter",
    "truncate",
    "grant",
    "revoke",
    "commit",
    "rollback",
)

BLOCKED_FUNCTIONS = (
    "dbms_scheduler",
    "utl_file",
    "execute immediate",
    "dbms_sql",
)
DEFAULT_SQL_LIMIT = 200


class SQLGenerationError(RuntimeError):
    """Raised when SQL candidate generation cannot produce valid options."""


def generate_sql_candidates(
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    llm_client: LLMClient | None = None,
    retry_context: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    """
    Generate exactly 3 Oracle SQL candidates.

    Contract target: generate_sql_candidates(user_request, requirements, metadata) -> list[dict]
    """
    if llm_client is None:
        raise SQLGenerationError("LLM client is required for SQL generation.")

    required_filters = _required_filters(
        requirements,
        metadata,
        user_request=user_request,
    )
    period_policy = _build_period_policy(requirements, metadata)
    granularity = _normalize_time_granularity(requirements.get("time_granularity"))
    granularity_rule_text = _granularity_bind_rule_text(granularity)
    candidates = _generate_with_llm(
        llm_client=llm_client,
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        required_filters=required_filters,
        retry_context=retry_context,
    )
    if len(candidates) < 3:
        raise SQLGenerationError(
            f"Model returned {len(candidates)} SQL candidate(s); expected exactly 3."
        )

    sanitized: list[dict[str, str]] = []
    for idx in range(3):
        base = candidates[idx]
        fixed = _validate_and_fix_candidate(
            base,
            idx=idx,
            required_filters=required_filters,
            llm_client=llm_client,
            period_policy=period_policy,
            granularity=granularity,
            granularity_rule_text=granularity_rule_text,
        )
        if fixed is None:
            raise SQLGenerationError(
                f"Candidate option_{idx + 1} failed validation/repair."
            )
        sanitized.append(fixed)

    if len(sanitized) != 3:
        raise SQLGenerationError("Could not produce exactly 3 valid SQL candidates.")
    return sanitized


def sanity_check_sql(sql: str) -> tuple[bool, str]:
    """Light SQL safety and sanity checks."""
    clean = _clean_sql(sql)
    if not clean:
        return False, "SQL is empty."

    if clean.count(";") > 0:
        return False, "Multiple SQL statements are not allowed."

    lowered = clean.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Only SELECT/CTE statements are allowed."

    if re.search(r"\bselect\s+\*", lowered):
        return False, "SELECT * is not allowed."

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            return False, f"Blocked keyword detected: {keyword}"

    for fn_name in BLOCKED_FUNCTIONS:
        if fn_name in lowered:
            return False, f"Blocked function detected: {fn_name}"

    if "--" in clean or "/*" in clean or "*/" in clean:
        return False, "SQL comments are not allowed."

    return True, "ok"


def _generate_with_llm(
    *,
    llm_client: LLMClient,
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    required_filters: list[str],
    retry_context: dict[str, Any] | None,
) -> list[dict[str, str]]:
    prompt = _build_sql_prompt(
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        required_filters=required_filters,
        retry_context=retry_context,
    )
    raw = _call_llm(
        llm_client=llm_client,
        system_prompt="You are a senior Oracle SQL engineer. Return strict JSON only.",
        prompt=prompt,
        temperature=0.1,
        max_tokens=1800,
    )
    parsed = _parse_candidates_json(raw)
    if len(parsed) < 3:
        recovered = _recover_three_sql_candidates(
            llm_client=llm_client,
            raw_output=raw,
            required_filters=required_filters,
        )
        if recovered:
            parsed = recovered

    return parsed


def _build_sql_prompt(
    *,
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    required_filters: list[str],
    retry_context: dict[str, Any] | None,
) -> str:
    metadata_text = _metadata_prompt_text(metadata)
    sql_rule_text = _sql_rule_prompt_text(
        requirements,
        required_filters,
        metadata,
        user_request=user_request,
    )
    retry_text = _retry_context_prompt_text(retry_context)
    return (
        "You are a senior SQL engineer. "
        "Your task is to generate three recommended SQL queries based strictly on "
        "the provided metadata, request, rules and requirements.\n\n"
        "Rules:\n"
        "- Output ONLY SQL statements. No explanations, no markdown, no comments.\n"
        "- Use only tables, columns, and joins that exist in the metadata.\n"
        "- Prefer explicit JOINs; never use SELECT *.\n"
        "- Only include columns needed to satisfy the request.\n"
        "- Apply filters as early as possible (WHERE).\n"
        "- If aggregating, include proper GROUP BY.\n"
        "- If a field is ambiguous, choose the most reasonable based on metadata; do not ask questions.\n"
        "- Always return 3 SQL statements even when confidence is low. Prefer safe conservative queries over invalid markers.\n\n"
        "Optimization priorities:\n"
        "- Minimize scanned data (filters, selective columns).\n"
        "- Use sargable predicates (no functions on indexed columns if avoidable).\n"
        "- Avoid unnecessary subqueries; use CTEs only if it improves clarity or reuse.\n"
        "- Ensure join keys are indexed when possible (use PK/FK).\n\n"
        "Output format:\n"
        "- Return exactly 3 SQL statements.\n"
        "- End each SQL statement with a semicolon.\n"
        "- Do not return JSON, numbering, labels, or any extra text.\n\n"
        f"Metadata:\n{metadata_text}\n\n"
        f"Request:\n{user_request}\n\n"
        f"Sql Rule:\n{sql_rule_text}\n\n"
        f"{retry_text}"
        f"Requirements JSON:\n{compact_json(requirements)}"
    )


def _retry_context_prompt_text(retry_context: dict[str, Any] | None) -> str:
    if not isinstance(retry_context, dict):
        return ""
    if not retry_context:
        return ""

    disqualify_reasons = _as_string_list(retry_context.get("disqualify_reasons"))[:10]
    blocked_patterns = _as_string_list(retry_context.get("blocked_sql_patterns"))[:8]
    if not disqualify_reasons and not blocked_patterns:
        return ""

    lines = [
        "Retry Guidance (Attempt 2):",
        "- This is a retry after candidate disqualification and/or judge failure.",
    ]
    if disqualify_reasons:
        lines.append(f"- Previously observed disqualify reasons: {', '.join(disqualify_reasons)}")
    if blocked_patterns:
        lines.append(f"- Previously risky SQL patterns: {', '.join(blocked_patterns)}")
    lines.append("- Generate 3 alternatives that avoid these failures.")
    return "\n".join(lines) + "\n\n"


def _parse_candidates_json(raw: str) -> list[dict[str, str]]:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return _parse_candidates_from_text(text)

    return _parse_candidates_from_value(parsed)


def _parse_candidates_from_value(parsed: Any) -> list[dict[str, str]]:
    if isinstance(parsed, list):
        return _candidates_from_sql_list(parsed)

    if not isinstance(parsed, dict):
        return []

    items = parsed.get("candidates")
    if not isinstance(items, list):
        items = parsed.get("queries")
    if not isinstance(items, list):
        sql_1 = str(parsed.get("sql_1", "")).strip()
        sql_2 = str(parsed.get("sql_2", "")).strip()
        sql_3 = str(parsed.get("sql_3", "")).strip()
        return _candidates_from_sql_list([sql_1, sql_2, sql_3])

    candidates: list[dict[str, str]] = []
    for item in items:
        idx = len(candidates) + 1
        if idx > 3:
            break
        if isinstance(item, str):
            sql = item.strip()
            if not sql or _is_invalid_marker(sql):
                continue
            candidates.append(
                {
                    "id": f"option_{idx}",
                    "sql": _normalize_sql_text(sql),
                    "rationale_short": "",
                    "risk_notes": "",
                }
            )
            continue

        if not isinstance(item, dict):
            continue
        sql = str(item.get("sql", "")).strip()
        if not sql or _is_invalid_marker(sql):
            continue
        candidates.append(
            {
                "id": str(item.get("id") or f"option_{idx}"),
                "sql": _normalize_sql_text(sql),
                "rationale_short": str(item.get("rationale_short", "")).strip(),
                "risk_notes": str(item.get("risk_notes", "")).strip(),
            }
        )
    return candidates


def _candidates_from_sql_list(items: list[Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for item in items:
        idx = len(candidates) + 1
        if idx > 3:
            break
        sql = _normalize_sql_text(str(item))
        if not sql or _is_invalid_marker(sql):
            continue
        candidates.append(
            {
                "id": f"option_{idx}",
                "sql": sql,
                "rationale_short": "",
                "risk_notes": "",
            }
        )
    return candidates


def _parse_candidates_from_text(text: str) -> list[dict[str, str]]:
    statements = _extract_sql_statements(text)
    return _candidates_from_sql_list(statements)


def _recover_three_sql_candidates(
    *,
    llm_client: LLMClient,
    raw_output: str,
    required_filters: list[str],
) -> list[dict[str, str]]:
    prompt = (
        "Normalize this model output into exactly 3 Oracle SQL statements.\n"
        "Rules:\n"
        "- Return strict JSON only in schema: "
        '{"candidates":[{"id":"option_1","sql":"..."},{"id":"option_2","sql":"..."},{"id":"option_3","sql":"..."}]}\n'
        "- Use SELECT/CTE only.\n"
        "- Do not return INVALID_REQUEST.\n"
        f"- Every SQL must include all required filters and FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.\n\n"
        f"Required filters: {compact_json(required_filters)}\n\n"
        f"Raw model output:\n{raw_output}"
    )
    normalized = _call_llm(
        llm_client=llm_client,
        system_prompt="You are a strict SQL output normalizer.",
        prompt=prompt,
        temperature=0.0,
        max_tokens=1400,
    )
    return _parse_candidates_json(normalized)


def _extract_sql_statements(text: str) -> list[str]:
    cleaned = _strip_fence(text)
    cleaned = re.sub(
        r"(?im)^\s*(sql[_\s-]*\d+|option[_\s-]*\d+)\s*:\s*",
        "",
        cleaned,
    )

    statements = _split_sql_statements(cleaned)
    if not statements:
        statements = _split_by_select_start(cleaned)

    result: list[str] = []
    for stmt in statements:
        normalized = _normalize_sql_text(stmt)
        if not normalized or _is_invalid_marker(normalized):
            continue
        if re.match(r"^(select|with)\b", normalized, flags=re.IGNORECASE):
            result.append(normalized)
    return result


def _split_sql_statements(text: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    depth = 0

    for char in text:
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "(" and not in_single and not in_double:
            depth += 1
        elif char == ")" and not in_single and not in_double and depth > 0:
            depth -= 1

        if char == ";" and not in_single and not in_double and depth == 0:
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            continue

        buf.append(char)

    trailing = "".join(buf).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _split_by_select_start(text: str) -> list[str]:
    matches = list(re.finditer(r"(?i)\b(select|with)\b", text))
    if not matches:
        return []

    statements: list[str] = []
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        part = text[start:end].strip()
        if part:
            statements.append(part)
    return statements


def _is_invalid_marker(sql: str) -> bool:
    normalized = _normalize_space(sql).upper()
    return normalized == "INVALID_REQUEST"


def _normalize_sql_text(sql: str) -> str:
    text = str(sql).strip()
    text = re.sub(r"^\s*\d+\s*[\).\:-]\s*", "", text)
    text = re.sub(r";+\s*$", "", text)
    return text.strip()


def _validate_and_fix_candidate(
    candidate: dict[str, str],
    *,
    idx: int,
    required_filters: list[str],
    llm_client: LLMClient,
    period_policy: dict[str, Any],
    granularity: str,
    granularity_rule_text: str,
) -> dict[str, str] | None:
    fixed = {
        "id": str(candidate.get("id") or f"option_{idx + 1}"),
        "sql": _enforce_constraints(str(candidate.get("sql", ""))),
        "rationale_short": str(candidate.get("rationale_short", "")).strip()
        or "Alternative interpretation.",
        "risk_notes": str(candidate.get("risk_notes", "")).strip()
        or "Verify business definitions before execution.",
    }

    ok, reason = sanity_check_sql(fixed["sql"])
    period_reason = _period_policy_violation(fixed["sql"], period_policy)
    granularity_reason = _granularity_bind_violation(fixed["sql"], granularity)
    missing_filters = _find_missing_required_filters(
        fixed["sql"],
        required_filters=required_filters,
    )
    if ok and period_reason is None and granularity_reason is None and not missing_filters:
        return fixed
    failure_reasons: list[str] = []
    if not ok:
        failure_reasons.append(reason)
    if period_reason is not None:
        failure_reasons.append(period_reason)
    if granularity_reason is not None:
        failure_reasons.append(granularity_reason)
    if missing_filters:
        failure_reasons.append(
            f"Missing required filters: {', '.join(missing_filters)}"
        )
    failure_reason = " | ".join(failure_reasons) or "Unknown validation error."

    repaired = _repair_candidate_with_llm(
        llm_client=llm_client,
        candidate=fixed,
        failure_reason=failure_reason,
        required_filters=required_filters,
        missing_filters=missing_filters,
        period_policy=period_policy,
        granularity_rule_text=granularity_rule_text,
    )
    if repaired is None:
        return None

    ok2, _ = sanity_check_sql(repaired["sql"])
    period_reason2 = _period_policy_violation(repaired["sql"], period_policy)
    granularity_reason2 = _granularity_bind_violation(repaired["sql"], granularity)
    missing_filters2 = _find_missing_required_filters(
        repaired["sql"],
        required_filters=required_filters,
    )
    if ok2 and period_reason2 is None and granularity_reason2 is None and not missing_filters2:
        return repaired
    return None


def _repair_candidate_with_llm(
    *,
    llm_client: LLMClient,
    candidate: dict[str, str],
    failure_reason: str,
    required_filters: list[str],
    missing_filters: list[str],
    period_policy: dict[str, Any],
    granularity_rule_text: str,
) -> dict[str, str] | None:
    period_rule_text = ""
    if period_policy.get("enforce_ek_tanzim"):
        period_rule_text = (
            "\n- For this request, period filtering must use EK TANZIM date basis.\n"
            "- Do NOT use REPORT_PERIOD column predicates.\n"
            "- Use :report_period with EK TANZIM context "
            "(for example TANZIM_TARIH_ID or GNL_TARIH date joins).\n"
        )

    prompt = (
        "Repair this SQL candidate to satisfy strict safety rules.\n"
        "Return strict JSON only with fields id, sql, rationale_short, risk_notes.\n"
        f"Rules: Oracle SQL, SELECT/CTE only, no SELECT *, include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY, "
        f"include required filters.{period_rule_text}\n"
        f"{granularity_rule_text}"
        f"Failure reason: {failure_reason}\n"
        f"Required filters: {compact_json(required_filters)}\n"
        f"Missing filters to add/fix first: {compact_json(missing_filters)}\n"
        f"Candidate: {compact_json(candidate)}"
    )
    raw = _call_llm(
        llm_client=llm_client,
        system_prompt="You are a strict Oracle SQL validator.",
        prompt=prompt,
        temperature=0.0,
        max_tokens=1000,
    )

    parsed = _parse_single_candidate_json(raw)
    if parsed is None:
        return None
    parsed["sql"] = _enforce_constraints(parsed["sql"])
    return parsed


def _parse_single_candidate_json(raw: str) -> dict[str, str] | None:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    sql = str(parsed.get("sql", "")).strip()
    if not sql:
        return None
    return {
        "id": str(parsed.get("id", "option")),
        "sql": sql,
        "rationale_short": str(parsed.get("rationale_short", "")).strip(),
        "risk_notes": str(parsed.get("risk_notes", "")).strip(),
    }


def _required_filters(
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    *,
    user_request: str,
) -> list[str]:
    required: list[str] = []
    granularity = _normalize_time_granularity(requirements.get("time_granularity"))
    for flt in _normalized_filter_list(requirements.get("required_filters")):
        if flt not in required:
            required.append(flt)
    for flt in _normalized_filter_list(metadata.get("mandatory_rules")):
        if flt not in required:
            required.append(flt)
    for flt in _normalized_filter_list(metadata.get("runtime_mandatory_rules")):
        if flt not in required:
            required.append(flt)
    required = _prune_conflicting_time_bind_filters(required, granularity)

    runtime_rules: list[str] = []

    granular_filter = _build_granular_time_filter(
        requirements=requirements,
        metadata=metadata,
        user_request=user_request,
    )
    if granular_filter and granular_filter not in required:
        required.append(granular_filter)
    if granular_filter:
        runtime_rules.append(granular_filter)

    _set_runtime_mandatory_rules(metadata, runtime_rules)

    return required


def _enforce_constraints(sql: str) -> str:
    clean = _clean_sql(sql)
    clean = _ensure_row_limit(clean)
    return clean


def _clean_sql(sql: str) -> str:
    return _normalize_sql_text(_strip_fence(sql))


def _find_missing_required_filters(sql: str, *, required_filters: list[str]) -> list[str]:
    normalized_sql = _normalize_space(sql).lower()
    where_clause = _extract_where_clause(sql)
    missing: list[str] = []
    for flt in required_filters:
        obligation = _normalize_filter_expression(flt)
        if not obligation:
            continue
        if _required_filter_is_satisfied(
            sql=sql,
            normalized_sql=normalized_sql,
            where_clause=where_clause,
            obligation=obligation,
        ):
            continue
        missing.append(flt)
    return missing


def _prune_conflicting_time_bind_filters(
    required_filters: list[str],
    granularity: str,
) -> list[str]:
    expected_bind = _expected_time_bind_for_granularity(granularity)
    if not expected_bind:
        return required_filters

    disallowed_binds = {"report_period", "year_value", "date_value"}
    disallowed_binds.discard(expected_bind)
    cleaned: list[str] = []
    for flt in required_filters:
        lowered = str(flt).lower()
        if any(re.search(rf":{bind}\b", lowered) for bind in disallowed_binds):
            continue
        cleaned.append(flt)
    return cleaned


def _granularity_bind_violation(sql: str, granularity: str) -> str | None:
    expected_bind = _expected_time_bind_for_granularity(granularity)
    if not expected_bind:
        return None

    sql_low = sql.lower()
    disallowed_binds = {"report_period", "year_value", "date_value"}
    disallowed_binds.discard(expected_bind)
    used_disallowed = sorted(
        bind for bind in disallowed_binds if re.search(rf":{bind}\b", sql_low)
    )
    if used_disallowed:
        return (
            f"Granularity '{granularity}' requires :{expected_bind}; "
            f"disallowed binds found: {', '.join(f':{item}' for item in used_disallowed)}."
        )
    if not re.search(rf":{expected_bind}\b", sql_low):
        return f"Granularity '{granularity}' requires :{expected_bind} bind variable."
    return None


def _expected_time_bind_for_granularity(granularity: str) -> str:
    if granularity == "year":
        return "year_value"
    if granularity == "month":
        return "report_period"
    if granularity == "day":
        return "date_value"
    return ""


def _required_filter_is_satisfied(
    *,
    sql: str,
    normalized_sql: str,
    where_clause: str,
    obligation: str,
) -> bool:
    normalized_obligation = _normalize_space(obligation).lower()
    if not normalized_obligation:
        return True

    if normalized_obligation in normalized_sql:
        return True

    if _matches_granular_time_obligation(sql, obligation):
        return True
    if _is_granular_time_obligation(obligation):
        return False

    bind_name = _extract_bind_token(obligation)
    column_name = _extract_column_token(obligation)
    if bind_name and column_name:
        escaped_column = re.escape(column_name)
        escaped_bind = re.escape(bind_name)
        pair_patterns = (
            rf"(?:\b[A-Za-z_][A-Za-z0-9_$#]*\.)?{escaped_column}\b\s*(?:=|<>|!=|<=|>=|<|>|like)\s*:{escaped_bind}\b",
            rf":{escaped_bind}\b\s*=\s*(?:\b[A-Za-z_][A-Za-z0-9_$#]*\.)?{escaped_column}\b",
        )
        for pattern in pair_patterns:
            if re.search(pattern, where_clause, flags=re.IGNORECASE):
                return True

    if bind_name and not column_name and re.search(
        rf":{re.escape(bind_name)}\b",
        where_clause,
        flags=re.IGNORECASE,
    ):
        return True

    if column_name and not bind_name and re.search(
        rf"(?:\b[A-Za-z_][A-Za-z0-9_$#]*\.)?{re.escape(column_name)}\b",
        where_clause,
        flags=re.IGNORECASE,
    ):
        return True

    return False


def _extract_bind_token(obligation: str) -> str:
    match = re.search(r":([A-Za-z_][A-Za-z0-9_]*)\b", obligation)
    if not match:
        return ""
    return match.group(1)


def _extract_column_token(obligation: str) -> str:
    lhs = re.split(
        r"(=|<>|!=|<=|>=|<|>|\blike\b|\bbetween\b|\bin\b|\bis\b)",
        obligation,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    candidates = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", lhs)
    if not candidates:
        return ""
    return candidates[-1]


def _is_granular_time_obligation(obligation: str) -> bool:
    lowered = _normalize_space(obligation).lower()
    if ":year_value" in lowered or ":date_value" in lowered:
        return True
    return ":report_period" in lowered and "to_char" in lowered


def _matches_granular_time_obligation(sql: str, obligation: str) -> bool:
    lowered = _normalize_space(obligation).lower()
    if ":year_value" in lowered:
        return _matches_to_char_bind(sql, "yyyy", "year_value")
    if ":date_value" in lowered:
        return _matches_to_char_bind(sql, "yyyymmdd", "date_value")
    if ":report_period" in lowered and "to_char" in lowered:
        return _matches_to_char_bind(sql, "yyyymm", "report_period")
    return False


def _matches_to_char_bind(sql: str, format_mask: str, bind_name: str) -> bool:
    to_char_expr = (
        rf"to_char\s*\(\s*[^,]+?\s*,\s*'{re.escape(format_mask)}'\s*\)"
    )
    left_pattern = rf"{to_char_expr}\s*=\s*:{re.escape(bind_name)}\b"
    right_pattern = rf":{re.escape(bind_name)}\b\s*=\s*{to_char_expr}"
    return bool(
        re.search(left_pattern, sql, flags=re.IGNORECASE)
        or re.search(right_pattern, sql, flags=re.IGNORECASE)
    )


def _ensure_row_limit(sql: str) -> str:
    pattern = r"\bfetch\s+first\s+(?::\w+|\d+)\s+rows\s+only\b"
    if re.search(pattern, sql, re.IGNORECASE):
        return re.sub(
            pattern,
            f"FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY",
            sql,
            flags=re.IGNORECASE,
        )
    return f"{sql} FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY"


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_where_clause(sql: str) -> str:
    match = re.search(
        r"\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\bhaving\b|\bfetch\s+first\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return match.group(1)


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _metadata_prompt_text(metadata: dict[str, Any]) -> str:
    lines: list[str] = []
    dialect = str(metadata.get("dialect", "oracle sql")).strip() or "oracle sql"
    lines.append(f"- Dialect: {dialect}")
    retrieval_debug = metadata.get("retrieval_debug")
    if isinstance(retrieval_debug, dict):
        metadata_source = str(retrieval_debug.get("metadata_source", "")).strip()
        if metadata_source:
            lines.append(f"- Metadata source: {metadata_source}")
    lines.append("- Tables:")

    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list) or not relevant:
        lines.append("  - No relevant metadata found.")
    else:
        for item in relevant:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table", "")).strip()
            if not table:
                continue
            columns = _metadata_columns_text(item.get("columns"))
            if columns:
                lines.append(f"  - {table}({columns})")
            else:
                lines.append(f"  - {table}")

            joins = _as_string_list(item.get("joins"))
            if joins:
                lines.append(f"    Joins: {', '.join(joins)}")
            indexes = _as_string_list(item.get("indexes"))
            if indexes:
                lines.append(f"    Indexes: {', '.join(indexes)}")

    mandatory_rules = _as_string_list(metadata.get("mandatory_rules"))
    if mandatory_rules:
        lines.append(f"- Mandatory rules: {', '.join(mandatory_rules)}")
    runtime_mandatory_rules = _as_string_list(metadata.get("runtime_mandatory_rules"))
    if runtime_mandatory_rules:
        lines.append(
            f"- Runtime mandatory rules: {', '.join(runtime_mandatory_rules)}"
        )

    guardrails = _as_string_list(metadata.get("guardrails"))
    if guardrails:
        lines.append(f"- Guardrails: {', '.join(guardrails)}")

    return "\n".join(lines)


def _metadata_columns_text(columns: Any) -> str:
    if not isinstance(columns, list):
        return ""
    out: list[str] = []
    for col in columns[:80]:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name", "")).strip()
        col_type = str(col.get("type", "")).strip()
        if not name:
            continue
        base = f"{name} {col_type}".strip()
        extras: list[str] = []

        description = _short_metadata_text(col.get("description"), limit=120)
        if description:
            extras.append(f"desc: {description}")

        semantic_type = _short_metadata_text(col.get("semantic_type"), limit=60)
        if semantic_type:
            extras.append(f"semantic_type: {semantic_type}")

        keywords = _as_string_list(col.get("keywords"))
        if keywords:
            extras.append(
                f"keywords: {', '.join(_short_metadata_text(item, limit=24) for item in keywords[:6])}"
            )

        properties = _as_string_list(col.get("properties"))
        if properties:
            extras.append(
                f"properties: {', '.join(_short_metadata_text(item, limit=48) for item in properties[:5])}"
            )

        if extras:
            out.append(f"{base} [{'; '.join(extras)}]")
        else:
            out.append(base)
    return ", ".join(out)


def _short_metadata_text(value: Any, *, limit: int) -> str:
    text = _normalize_space(str(value)).strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 3)]}..."


def _sql_rule_prompt_text(
    requirements: dict[str, Any],
    required_filters: list[str],
    metadata: dict[str, Any],
    *,
    user_request: str,
) -> str:
    lines: list[str] = [
        "- Oracle SQL dialect is mandatory.",
        f"- Every query must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.",
        "- Every query must include all mandatory filters in WHERE.",
        "- Use bind placeholders for runtime values when possible (for example :report_period).",
    ]

    if required_filters:
        lines.append(f"- Mandatory filters: {', '.join(required_filters)}")
    else:
        lines.append("- Mandatory filters: none")

    report_period = str(requirements.get("report_period", "")).strip()
    if report_period:
        lines.append(f"- Runtime hint report_period: {report_period}")
        period_policy = _build_period_policy(requirements, metadata)
        if period_policy.get("enforce_ek_tanzim"):
            lines.append(
                "- Uretim agent rule: use EK TANZIM date basis for period filtering."
            )
            lines.append(
                "- Do not use REPORT_PERIOD column; use EK TANZIM context "
                "(for example TANZIM_TARIH_ID / GNL_TARIH joins) with :report_period."
            )

    granular_context = _granular_time_context(
        requirements=requirements,
        metadata=metadata,
        user_request=user_request,
    )
    granularity = str(granular_context.get("granularity", "")).strip()
    time_value = str(granular_context.get("time_value", "")).strip()
    if granularity:
        lines.append(f"- Runtime hint time_granularity: {granularity}")
        lines.append(_granularity_bind_rule_text(granularity).strip())
    if time_value:
        lines.append(f"- Runtime hint time_value: {time_value}")

    if granular_context.get("tanzim_signal") and granular_context.get("tanzim_path_available"):
        lines.append(
            "- Tanzim-period signal detected: prioritize TANZIM_TARIH_ID -> GNL_TARIH.TARIH path."
        )

    generated_filter = str(granular_context.get("mandatory_filter", "")).strip()
    if generated_filter:
        lines.append(f"- Granular time mandatory filter: {generated_filter}")
        if granular_context.get("target_kind") == "date_like_number":
            lines.append(
                "- Selected time target is controlled date-like NUMBER column; use digit-safe conversion semantics."
            )
    elif granularity:
        lines.append(
            "- Granular time mandatory filter not generated because no eligible time target column was selected from metadata."
        )

    time_range = requirements.get("time_range")
    if isinstance(time_range, dict):
        start = str(time_range.get("start", "")).strip()
        end = str(time_range.get("end", "")).strip()
        if start:
            lines.append(f"- Runtime hint start_date: {start}")
        if end:
            lines.append(f"- Runtime hint end_date: {end}")

    return "\n".join(lines)


def _granularity_bind_rule_text(granularity: str) -> str:
    if granularity == "year":
        return (
            "- Granularity bind policy: use :year_value only for period filtering; "
            "do not use :report_period or :date_value.\n"
        )
    if granularity == "month":
        return (
            "- Granularity bind policy: use :report_period only for period filtering; "
            "do not use :year_value or :date_value.\n"
        )
    if granularity == "day":
        return (
            "- Granularity bind policy: use :date_value only for period filtering; "
            "do not use :year_value or :report_period.\n"
        )
    return ""


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
        return _dedupe(out)
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


def _set_runtime_mandatory_rules(
    metadata: dict[str, Any],
    runtime_rules: list[str],
) -> None:
    merged: list[str] = []
    for flt in _normalized_filter_list(metadata.get("runtime_mandatory_rules")):
        if flt not in merged:
            merged.append(flt)
    for flt in _normalized_filter_list(runtime_rules):
        if flt not in merged:
            merged.append(flt)
    metadata["runtime_mandatory_rules"] = merged


def _build_granular_time_filter(
    *,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    user_request: str,
) -> str | None:
    context = _granular_time_context(
        requirements=requirements,
        metadata=metadata,
        user_request=user_request,
    )
    filter_expr = str(context.get("mandatory_filter", "")).strip()
    if filter_expr:
        return filter_expr
    return None


def _granular_time_context(
    *,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    user_request: str,
) -> dict[str, Any]:
    granularity = _normalize_time_granularity(requirements.get("time_granularity"))
    report_period = _normalize_report_period(requirements.get("report_period"))
    time_value = _normalize_time_value(requirements.get("time_value"))

    if not granularity and report_period:
        granularity = "month"
        time_value = report_period

    bind_name = ""
    bind_value = ""
    format_mask = ""
    if granularity == "year":
        bind_name = "year_value"
        bind_value = time_value if _is_year_value(time_value) else ""
        format_mask = "yyyy"
    elif granularity == "month":
        bind_name = "report_period"
        bind_value = report_period or (time_value if _is_month_value(time_value) else "")
        format_mask = "yyyymm"
    elif granularity == "day":
        bind_name = "date_value"
        bind_value = time_value if _is_day_value(time_value) else ""
        format_mask = "yyyymmdd"

    tanzim_signal = _request_has_tanzim_signal(user_request)
    tanzim_path_available = _has_tanzim_to_gnl_tarih_path(metadata)
    target = _select_time_target_column(
        metadata=metadata,
        prefer_tanzim_path=tanzim_signal and tanzim_path_available,
        user_request=user_request,
    )

    mandatory_filter = ""
    if granularity and bind_name and bind_value and target:
        mandatory_filter = _build_granularity_filter_expression(
            expression=target["expression"],
            target_kind=target["target_kind"],
            granularity=granularity,
            bind_name=bind_name,
            format_mask=format_mask,
        )

    return {
        "granularity": granularity,
        "time_value": bind_value,
        "bind_name": bind_name,
        "target_column": target["expression"] if target else "",
        "target_kind": target["target_kind"] if target else "",
        "tanzim_signal": tanzim_signal,
        "tanzim_path_available": tanzim_path_available,
        "mandatory_filter": mandatory_filter,
    }


def _build_granularity_filter_expression(
    *,
    expression: str,
    target_kind: str,
    granularity: str,
    bind_name: str,
    format_mask: str,
) -> str:
    if target_kind == "native_date":
        return f"TO_CHAR({expression}, '{format_mask}') = :{bind_name}"

    digits_expr = f"REGEXP_REPLACE(TRIM(TO_CHAR({expression})), '[^0-9]', '')"
    if granularity == "year":
        return f"SUBSTR({digits_expr}, 1, 4) = :{bind_name}"
    if granularity == "month":
        return f"SUBSTR({digits_expr}, 1, 6) = :{bind_name}"
    return f"SUBSTR({digits_expr}, 1, 8) = :{bind_name}"


def _normalize_time_granularity(value: Any) -> str:
    text = _normalize_space(str(value or "")).lower()
    if text in ("year", "yyyy"):
        return "year"
    if text in ("month", "yyyymm"):
        return "month"
    if text in ("day", "yyyymmdd"):
        return "day"
    return ""


def _normalize_time_value(value: Any) -> str:
    return re.sub(r"\D", "", _normalize_space(str(value or "")))


def _normalize_report_period(value: Any) -> str:
    digits = _normalize_time_value(value)
    return digits if _is_month_value(digits) else ""


def _is_year_value(value: str) -> bool:
    return bool(re.fullmatch(r"20\d{2}", value))


def _is_month_value(value: str) -> bool:
    return bool(re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])", value))


def _is_day_value(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"20\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])",
            value,
        )
    )


def _request_has_tanzim_signal(user_request: str) -> bool:
    lowered = user_request.lower()
    signals = (
        "tanzim donemi",
        "tanzim tarihi",
        "ek tanzim",
        "tanzim",
    )
    return any(signal in lowered for signal in signals)
def _has_tanzim_to_gnl_tarih_path(metadata: dict[str, Any]) -> bool:
    relevant_items = metadata.get("relevant_items")
    if not isinstance(relevant_items, list):
        return False
    text_chunks: list[str] = []
    for item in relevant_items:
        if not isinstance(item, dict):
            continue
        text_chunks.extend(_as_string_list(item.get("joins")))
        text_chunks.extend(_as_string_list(item.get("relationships")))

    merged = " ".join(text_chunks).lower()
    return (
        "tanzim_tarih_id" in merged
        and "gnl_tarih" in merged
        and "tarih_id" in merged
    )


def _select_time_target_column(
    *,
    metadata: dict[str, Any],
    prefer_tanzim_path: bool,
    user_request: str,
) -> dict[str, str] | None:
    candidates = _collect_time_target_columns(
        metadata=metadata,
        prefer_tanzim_path=prefer_tanzim_path,
        user_request=user_request,
    )
    if not candidates:
        return None
    return candidates[0]
def _collect_time_target_columns(
    *,
    metadata: dict[str, Any],
    prefer_tanzim_path: bool,
    user_request: str,
) -> list[dict[str, Any]]:
    relevant_items = metadata.get("relevant_items")
    if not isinstance(relevant_items, list):
        return []
    candidates: list[dict[str, Any]] = []
    request_low = user_request.lower()
    tanzim_path_available = _has_tanzim_to_gnl_tarih_path(metadata)
    for item_rank, item in enumerate(relevant_items):
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        item_score = _safe_float(item.get("score"))
        joins_text = " ".join(_as_string_list(item.get("joins"))).lower()
        columns = item.get("columns")
        if not isinstance(columns, list):
            continue
        for column in columns:
            if not isinstance(column, dict):
                continue
            column_name = str(column.get("name", "")).strip()
            column_type = str(column.get("type", "")).strip()
            if not column_name:
                continue
            target_kind = ""
            if _is_date_or_timestamp_type(column_type):
                target_kind = "native_date"
            elif _is_controlled_date_like_column(
                table=table,
                column=column,
                prefer_tanzim_path=prefer_tanzim_path,
                tanzim_path_available=tanzim_path_available,
            ):
                target_kind = "date_like_number"
            if not target_kind:
                continue
            expression = f"{table}.{column_name}" if table else column_name
            signal_score = 100.0 if target_kind == "native_date" else 70.0
            if _is_tanzim_target_candidate(table, column_name):
                signal_score += 20.0
            if prefer_tanzim_path and _is_tanzim_target_candidate(table, column_name):
                signal_score += 80.0
            if table and table.lower() in request_low:
                signal_score += 10.0
            if column_name.lower() in request_low:
                signal_score += 8.0
            if "gnl_tarih" in joins_text and "tarih_id" in joins_text:
                signal_score += 4.0
            signal_score += max(0.0, item_score) * 10.0
            candidates.append(
                {
                    "table": table,
                    "column": column_name,
                    "type": column_type,
                    "expression": expression,
                    "target_kind": target_kind,
                    "item_rank": item_rank,
                    "signal_score": signal_score,
                }
            )
    candidates.sort(
        key=lambda item: (
            0
            if prefer_tanzim_path
            and _is_tanzim_target_candidate(item["table"], item["column"])
            else 1,
            -float(item["signal_score"]),
            int(item["item_rank"]),
            item["table"].lower(),
            item["column"].lower(),
        )
    )
    return candidates
def _is_date_or_timestamp_type(column_type: str) -> bool:
    lowered = _normalize_space(column_type).lower()
    return "date" in lowered or "timestamp" in lowered
def _is_controlled_date_like_column(
    *,
    table: str,
    column: dict[str, Any],
    prefer_tanzim_path: bool,
    tanzim_path_available: bool,
) -> bool:
    column_name = str(column.get("name", "")).strip()
    column_type = str(column.get("type", "")).strip()
    if not column_name or not _is_numeric_type(column_type):
        return False
    if table.lower() == "as_dwh.gnl_tarih" and column_name.lower() == "tarih":
        return True
    if _is_tanzim_target_candidate(table, column_name) and tanzim_path_available:
        return True
    signal_parts = [
        column_name,
        str(column.get("description", "")),
        str(column.get("semantic_type", "")),
        " ".join(_as_string_list(column.get("keywords"))),
        " ".join(_as_string_list(column.get("properties"))),
    ]
    signal_text = _normalize_space(" ".join(signal_parts)).lower()
    has_date_word = any(
        token in signal_text
        for token in ("tarih", "date", "yil", "ay", "gun", "period")
    )
    has_format_signal = any(
        token in signal_text
        for token in (
            "yyyymmdd",
            "yyyymm",
            "yyyy-mm",
            "yyyy/mm",
            "date key",
            "date_id",
        )
    )
    if has_date_word and has_format_signal:
        return True
    if prefer_tanzim_path and has_date_word:
        return True
    return False
def _is_numeric_type(column_type: str) -> bool:
    lowered = _normalize_space(column_type).lower()
    return any(token in lowered for token in ("number", "numeric", "decimal", "int"))
def _is_tanzim_target_candidate(table: str, column_name: str) -> bool:
    table_low = table.lower()
    column_low = column_name.lower()
    if table_low.endswith("gnl_tarih") and column_low == "tarih":
        return True
    if "tanzim" in column_low and "tarih" in column_low:
        return True
    return column_low == "tarih_id"
def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
def _call_llm(
    *,
    llm_client: LLMClient,
    system_prompt: str,
    prompt: str,
    temperature: float,
    max_tokens: int,
) -> str:
    try:
        return llm_client.chat(
            system_prompt,
            prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )
    except LLMError as exc:
        raise SQLGenerationError(f"LLM request failed: {exc}") from exc


def _normalized_filter_list(value: Any) -> list[str]:
    normalized: list[str] = []
    for text in _as_string_list(value):
        expr = _normalize_filter_expression(text)
        if expr and expr not in normalized:
            normalized.append(expr)
    return normalized


def _normalize_filter_expression(filter_text: str) -> str:
    text = _normalize_space(str(filter_text))
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


def _build_period_policy(
    requirements: dict[str, Any],
    metadata: dict[str, Any],
) -> dict[str, Any]:
    report_period = str(requirements.get("report_period", "")).strip()
    if not report_period:
        return {"enforce_ek_tanzim": False}
    if not _is_uretim_metadata(metadata):
        return {"enforce_ek_tanzim": False}
    if not _metadata_prefers_ek_tanzim(metadata):
        return {"enforce_ek_tanzim": False}
    return {
        "enforce_ek_tanzim": True,
        "report_period": report_period,
    }


def _period_policy_violation(sql: str, period_policy: dict[str, Any]) -> str | None:
    if not period_policy.get("enforce_ek_tanzim"):
        return None

    lowered = sql.lower()
    if re.search(r"(?<!:)\breport_period\b", lowered):
        return "Uretim period filters must not use REPORT_PERIOD column."

    if ":report_period" not in lowered:
        return "Uretim period filters must use :report_period bind variable."

    if not any(token in lowered for token in ("tanzim_tarih_id", "ek_tanzim", "gnl_tarih")):
        return (
            "Uretim period filters must reference EK TANZIM date context "
            "(TANZIM_TARIH_ID, EK_TANZIM alias, or GNL_TARIH)."
        )

    return None


def _is_uretim_metadata(metadata: dict[str, Any]) -> bool:
    retrieval_debug = metadata.get("retrieval_debug")
    if isinstance(retrieval_debug, dict):
        source = str(retrieval_debug.get("metadata_source", "")).lower()
        if "uretim" in source:
            return True
    return False


def _metadata_prefers_ek_tanzim(metadata: dict[str, Any]) -> bool:
    texts: list[str] = _as_string_list(metadata.get("guardrails"))
    relevant = metadata.get("relevant_items")
    if isinstance(relevant, list):
        for item in relevant:
            if not isinstance(item, dict):
                continue
            texts.extend(_as_string_list(item.get("performance_rules")))

    merged = _normalize_space(" ".join(texts)).lower()
    return "ek tanzim" in merged


