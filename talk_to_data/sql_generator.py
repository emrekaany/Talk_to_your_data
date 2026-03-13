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
) -> list[dict[str, str]]:
    """
    Generate exactly 3 Oracle SQL candidates.

    Contract target: generate_sql_candidates(user_request, requirements, metadata) -> list[dict]
    """
    if llm_client is None:
        raise SQLGenerationError("LLM client is required for SQL generation.")

    required_filters = _required_filters(requirements, metadata)
    candidates = _generate_with_llm(
        llm_client=llm_client,
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        required_filters=required_filters,
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
) -> list[dict[str, str]]:
    prompt = _build_sql_prompt(
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        required_filters=required_filters,
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
) -> str:
    metadata_text = _metadata_prompt_text(metadata)
    sql_rule_text = _sql_rule_prompt_text(requirements, required_filters)
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
        f"Requirements JSON:\n{compact_json(requirements)}"
    )


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
) -> dict[str, str] | None:
    fixed = {
        "id": str(candidate.get("id") or f"option_{idx + 1}"),
        "sql": _enforce_constraints(
            str(candidate.get("sql", "")),
            required_filters=required_filters,
        ),
        "rationale_short": str(candidate.get("rationale_short", "")).strip()
        or "Alternative interpretation.",
        "risk_notes": str(candidate.get("risk_notes", "")).strip()
        or "Verify business definitions before execution.",
    }

    ok, reason = sanity_check_sql(fixed["sql"])
    if ok:
        return fixed

    repaired = _repair_candidate_with_llm(
        llm_client=llm_client,
        candidate=fixed,
        failure_reason=reason,
        required_filters=required_filters,
    )
    if repaired is None:
        return None

    ok2, _ = sanity_check_sql(repaired["sql"])
    if ok2:
        return repaired
    return None


def _repair_candidate_with_llm(
    *,
    llm_client: LLMClient,
    candidate: dict[str, str],
    failure_reason: str,
    required_filters: list[str],
) -> dict[str, str] | None:
    prompt = (
        "Repair this SQL candidate to satisfy strict safety rules.\n"
        "Return strict JSON only with fields id, sql, rationale_short, risk_notes.\n"
        f"Rules: Oracle SQL, SELECT/CTE only, no SELECT *, include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY, "
        "include required filters.\n"
        f"Failure reason: {failure_reason}\n"
        f"Required filters: {compact_json(required_filters)}\n"
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
    parsed["sql"] = _enforce_constraints(parsed["sql"], required_filters=required_filters)
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


def _required_filters(requirements: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    required: list[str] = []
    for flt in _normalized_filter_list(requirements.get("required_filters")):
        if flt not in required:
            required.append(flt)
    for flt in _normalized_filter_list(metadata.get("mandatory_rules")):
        if flt not in required:
            required.append(flt)
    return required


def _enforce_constraints(sql: str, *, required_filters: list[str]) -> str:
    clean = _clean_sql(sql)
    clean = _ensure_required_filters(clean, required_filters)
    clean = _ensure_row_limit(clean)
    return clean


def _clean_sql(sql: str) -> str:
    return _normalize_sql_text(_strip_fence(sql))


def _ensure_required_filters(sql: str, required_filters: list[str]) -> str:
    missing = _missing_filters(sql, required_filters)
    if not missing:
        return sql

    split_match = re.search(
        r"\b(group\s+by|having|order\s+by|fetch\s+first)\b",
        sql,
        flags=re.IGNORECASE,
    )
    if split_match:
        split_idx = split_match.start()
        head = sql[:split_idx].rstrip()
        tail = sql[split_idx:].lstrip()
    else:
        head = sql.rstrip()
        tail = ""

    if re.search(r"\bwhere\b", head, flags=re.IGNORECASE):
        head = f"{head} AND {' AND '.join(missing)}"
    else:
        head = f"{head} WHERE {' AND '.join(missing)}"

    if tail:
        return f"{head} {tail}"
    return head


def _missing_filters(sql: str, required_filters: list[str]) -> list[str]:
    where_clause = _extract_where_clause(sql)
    normalized_where = _normalize_space(where_clause).lower()
    missing: list[str] = []
    for flt in required_filters:
        normalized_filter = _normalize_space(_normalize_filter_expression(flt)).lower()
        if not normalized_filter:
            continue
        if normalized_filter not in normalized_where:
            missing.append(flt)
    return missing


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


def _sql_rule_prompt_text(requirements: dict[str, Any], required_filters: list[str]) -> str:
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

    time_range = requirements.get("time_range")
    if isinstance(time_range, dict):
        start = str(time_range.get("start", "")).strip()
        end = str(time_range.get("end", "")).strip()
        if start:
            lines.append(f"- Runtime hint start_date: {start}")
        if end:
            lines.append(f"- Runtime hint end_date: {end}")

    return "\n".join(lines)


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
