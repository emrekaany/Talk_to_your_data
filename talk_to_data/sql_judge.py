"""LLM-based SQL option judge with deterministic local fallback."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any

from .llm_client import LLMClient, LLMError, compact_json
from .sql_generator import sanity_check_sql
from .sql_guardrails import SQLGuardrailError, validate_sql_before_execution


MAX_JUDGE_TOKENS = 32
_OPTION_PATTERN = re.compile(r"\boption_[123]\b", flags=re.IGNORECASE)
_AGGREGATION_TOKENS = (
    "sum",
    "total",
    "toplam",
    "count",
    "adet",
    "avg",
    "average",
    "ortalama",
    "min",
    "max",
)
_DETAIL_TOKENS = ("detay", "detail", "satir", "row", "liste", "list")
_ALIAS_STOPWORDS = {
    "on",
    "where",
    "group",
    "order",
    "fetch",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "join",
}


def select_best_sql_option_id(
    user_request: str,
    metadata_used: dict[str, Any],
    candidates: list[dict[str, Any]],
    llm_client: LLMClient | None = None,
) -> str:
    """Return best candidate id, using LLM judge first then deterministic fallback."""
    result = choose_best_sql_candidate(
        user_request=user_request,
        metadata_used=metadata_used,
        candidates=candidates,
        llm_client=llm_client,
    )
    return str(result["recommended_candidate_id"])


def choose_best_sql_candidate(
    *,
    user_request: str,
    metadata_used: dict[str, Any],
    candidates: list[dict[str, Any]],
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """
    Evaluate SQL candidates and return recommendation details.

    Output shape:
    {
      "recommended_candidate_id": "option_2",
      "recommended_canonical_id": "option_2",
      "selection_mode": "llm_judge" | "fallback",
      "fallback_reason": "...",
      "llm_raw_output": "...",
      "candidate_evaluations": [...]
    }
    """
    normalized_candidates = _normalize_candidates(candidates)
    if not normalized_candidates:
        return {
            "recommended_candidate_id": "option_1",
            "recommended_canonical_id": "option_1",
            "selection_mode": "fallback",
            "fallback_reason": "No candidates provided.",
            "llm_raw_output": "",
            "candidate_evaluations": [],
        }

    evaluations = _evaluate_candidates(
        user_request=user_request,
        metadata_used=metadata_used,
        candidates=normalized_candidates,
    )
    evaluation_by_canonical = {
        str(item["canonical_id"]).lower(): item for item in evaluations
    }

    llm_raw_output = ""
    llm_choice_canonical: str | None = None
    fallback_reason = ""

    if llm_client is not None:
        try:
            llm_raw_output = _call_llm_judge(
                llm_client=llm_client,
                user_request=user_request,
                metadata_used=metadata_used,
                candidates=normalized_candidates,
            )
            parsed = _parse_option_id(llm_raw_output)
            if parsed is not None:
                eval_item = evaluation_by_canonical.get(parsed.lower())
                if eval_item is not None and not bool(eval_item["hard_disqualified"]):
                    llm_choice_canonical = parsed
                else:
                    fallback_reason = (
                        "LLM selected a hard-disqualified option; fallback applied."
                    )
            else:
                fallback_reason = "LLM output could not be parsed; fallback applied."
        except LLMError as exc:
            fallback_reason = f"LLM judge failed: {exc}"
    else:
        fallback_reason = "LLM client unavailable; fallback applied."

    if llm_choice_canonical is not None:
        chosen = evaluation_by_canonical.get(llm_choice_canonical.lower())
        if chosen is not None:
            return {
                "recommended_candidate_id": str(chosen["candidate_id"]),
                "recommended_canonical_id": str(chosen["canonical_id"]),
                "selection_mode": "llm_judge",
                "fallback_reason": "",
                "llm_raw_output": llm_raw_output,
                "candidate_evaluations": evaluations,
            }

    chosen = _fallback_pick(evaluations)
    if chosen is None:
        first = normalized_candidates[0]
        return {
            "recommended_candidate_id": str(first["candidate_id"]),
            "recommended_canonical_id": str(first["canonical_id"]),
            "selection_mode": "fallback",
            "fallback_reason": fallback_reason or "No candidate evaluation available.",
            "llm_raw_output": llm_raw_output,
            "candidate_evaluations": evaluations,
        }

    return {
        "recommended_candidate_id": str(chosen["candidate_id"]),
        "recommended_canonical_id": str(chosen["canonical_id"]),
        "selection_mode": "fallback",
        "fallback_reason": fallback_reason or "Fallback policy selected the best candidate.",
        "llm_raw_output": llm_raw_output,
        "candidate_evaluations": evaluations,
    }


def _normalize_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for index, raw in enumerate(candidates[:3], start=1):
        if not isinstance(raw, dict):
            continue
        normalized.append(
            {
                "canonical_id": f"option_{index}",
                "candidate_id": str(raw.get("id", f"option_{index}")).strip() or f"option_{index}",
                "sql": str(raw.get("sql", "")).strip(),
                "explanation": str(raw.get("description", "")).strip(),
                "rationale_short": str(raw.get("rationale_short", "")).strip(),
                "risk_notes": str(raw.get("risk_notes", "")).strip(),
            }
        )
    return normalized


def _call_llm_judge(
    *,
    llm_client: LLMClient,
    user_request: str,
    metadata_used: dict[str, Any],
    candidates: list[dict[str, str]],
) -> str:
    system_prompt = (
        "You are a strict Oracle SQL candidate evaluator.\n"
        "You must pick exactly one best SQL option for the user request.\n"
        "Be conservative, safety-first, and metadata-grounded."
    )
    prompt = _build_judge_prompt(
        user_request=user_request,
        metadata_used=metadata_used,
        candidates=candidates,
    )
    return llm_client.chat(
        system_prompt,
        prompt,
        temperature=0.0,
        max_tokens=MAX_JUDGE_TOKENS,
    )


def _build_judge_prompt(
    *,
    user_request: str,
    metadata_used: dict[str, Any],
    candidates: list[dict[str, str]],
) -> str:
    option_map = {item["canonical_id"]: item for item in candidates}
    option_1 = option_map.get("option_1", {"sql": "", "explanation": ""})
    option_2 = option_map.get("option_2", {"sql": "", "explanation": ""})
    option_3 = option_map.get("option_3", {"sql": "", "explanation": ""})

    return (
        "Task:\n"
        "Select which SQL candidate best satisfies the request using the provided metadata context and constraints.\n\n"
        "Decision policy (hard rules):\n"
        "- Disqualify any candidate that violates mandatory filters.\n"
        "- Disqualify any candidate that conflicts with guardrails/security notes.\n"
        "- Disqualify any candidate that uses tables/columns not supported by metadata context.\n"
        "- Disqualify any candidate that does not match Oracle SELECT/CTE intent.\n\n"
        "Ranking criteria (in order):\n"
        "1) Request satisfaction and semantic correctness\n"
        "2) Mandatory filter compliance\n"
        "3) Correct grain/aggregation for the request intent\n"
        "4) Minimal unnecessary columns/joins\n"
        "5) Performance-safe structure (early filters, sensible grouping)\n\n"
        "Output rule:\n"
        "- Return ONLY one token: option_1 or option_2 or option_3\n"
        "- No JSON, no explanation, no extra text.\n\n"
        "Input:\n"
        "REQUEST:\n"
        f"{user_request}\n\n"
        "METADATA_CONTEXT_JSON:\n"
        f"{compact_json(metadata_used)}\n\n"
        "SQL_CANDIDATES:\n"
        "option_1:\n"
        f"SQL: {option_1['sql']}\n"
        f"EXPLANATION: {option_1['explanation']}\n\n"
        "option_2:\n"
        f"SQL: {option_2['sql']}\n"
        f"EXPLANATION: {option_2['explanation']}\n\n"
        "option_3:\n"
        f"SQL: {option_3['sql']}\n"
        f"EXPLANATION: {option_3['explanation']}"
    )


def _parse_option_id(text: str) -> str | None:
    match = _OPTION_PATTERN.search(text or "")
    if not match:
        return None
    return match.group(0).lower()


def _evaluate_candidates(
    *,
    user_request: str,
    metadata_used: dict[str, Any],
    candidates: list[dict[str, str]],
) -> list[dict[str, Any]]:
    required_filters = _normalized_filter_list(metadata_used.get("mandatory_rules"))
    request_tokens = _tokenize(user_request)
    needs_aggregation = _request_mentions_aggregation(user_request)
    needs_detail = _request_mentions_detail(user_request)

    evaluations: list[dict[str, Any]] = []
    for candidate in candidates:
        sql = str(candidate.get("sql", "")).strip()
        reasons: list[str] = []

        ok, reason = sanity_check_sql(sql)
        if not ok:
            reasons.append(f"Sanity check failed: {reason}")

        try:
            validate_sql_before_execution(sql, metadata_used, llm_client=None)
        except SQLGuardrailError as exc:
            reasons.append(str(exc))

        reasons.extend(_unknown_column_violations(sql, metadata_used))
        reasons.extend(_security_violations(sql, metadata_used))
        reasons = _dedupe_strings(reasons)

        score = _fallback_score(
            candidate=candidate,
            request_tokens=request_tokens,
            required_filters=required_filters,
            needs_aggregation=needs_aggregation,
            needs_detail=needs_detail,
        )
        evaluations.append(
            {
                "canonical_id": candidate["canonical_id"],
                "candidate_id": candidate["candidate_id"],
                "hard_disqualified": bool(reasons),
                "disqualify_reasons": reasons,
                "fallback_score": round(score, 6),
            }
        )

    return evaluations


def _unknown_column_violations(sql: str, metadata_used: dict[str, Any]) -> list[str]:
    table_columns = _metadata_table_columns(metadata_used)
    if not table_columns:
        return []

    alias_map = _extract_alias_map(sql, table_columns)
    if not alias_map:
        return []

    violations: list[str] = []
    for alias, column in re.findall(
        r"\b([A-Za-z_][A-Za-z0-9_$#]*)\.([A-Za-z_][A-Za-z0-9_$#]*)\b",
        sql,
        flags=re.IGNORECASE,
    ):
        alias_key = alias.lower()
        table_key = alias_map.get(alias_key)
        if not table_key:
            continue
        known_columns = table_columns.get(table_key)
        if not known_columns:
            continue
        if column.lower() in known_columns:
            continue
        violations.append(
            f"Unsupported column '{alias}.{column}' for metadata table '{table_key}'."
        )
    return _dedupe_strings(violations)


def _security_violations(sql: str, metadata_used: dict[str, Any]) -> list[str]:
    restricted_columns: list[str] = []
    for guardrail in _as_string_list(metadata_used.get("guardrails")):
        match = re.search(
            r"pii columns restricted\s*:\s*(.+)$",
            guardrail,
            flags=re.IGNORECASE,
        )
        if not match:
            continue
        for raw in match.group(1).split(","):
            token = raw.strip()
            if token:
                restricted_columns.append(token)

    items = metadata_used.get("relevant_items")
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            security = item.get("security")
            if not isinstance(security, dict):
                continue
            restricted_columns.extend(_as_string_list(security.get("pii_columns")))

    violations: list[str] = []
    for column in _dedupe_strings(restricted_columns):
        if re.search(rf"\b{re.escape(column)}\b", sql, flags=re.IGNORECASE):
            violations.append(f"PII-restricted column referenced: {column}")
    return violations


def _fallback_score(
    *,
    candidate: dict[str, str],
    request_tokens: Counter[str],
    required_filters: list[str],
    needs_aggregation: bool,
    needs_detail: bool,
) -> float:
    sql = candidate.get("sql", "")
    explanation_text = " ".join(
        [
            candidate.get("explanation", ""),
            candidate.get("rationale_short", ""),
            candidate.get("risk_notes", ""),
        ]
    )
    sql_tokens = _tokenize(sql)
    explanation_tokens = _tokenize(explanation_text)

    overlap_sql = _token_overlap(request_tokens, sql_tokens)
    overlap_explanation = _token_overlap(request_tokens, explanation_tokens)
    score = (2.0 * overlap_sql) + (0.5 * overlap_explanation)

    if required_filters:
        satisfied = _count_satisfied_filters(sql, required_filters)
        score += 4.0 * (satisfied / max(1, len(required_filters)))

    has_aggregation = bool(
        re.search(r"\b(sum|count|avg|min|max)\s*\(", sql, flags=re.IGNORECASE)
    )
    if needs_aggregation and has_aggregation:
        score += 3.0
    elif needs_aggregation and not has_aggregation:
        score -= 3.0
    elif needs_detail and has_aggregation:
        score -= 1.5

    lowered_sql = sql.lower()
    if " where " in f" {lowered_sql} ":
        score += 1.0
    if re.search(r"\bfetch\s+first\s+\d+\s+rows\s+only\b", lowered_sql):
        score += 1.0

    join_count = len(re.findall(r"\bjoin\b", lowered_sql))
    score -= max(0, join_count - 3) * 0.3

    select_count = _estimated_select_column_count(sql)
    score -= max(0, select_count - 15) * 0.05
    return score


def _count_satisfied_filters(sql: str, required_filters: list[str]) -> int:
    where_clause = _extract_where_clause(sql).lower()
    count = 0
    for flt in required_filters:
        normalized = _normalize_filter_expression(flt).lower()
        if not normalized:
            continue
        if normalized in where_clause:
            count += 1
            continue
        column = _extract_column_token(normalized)
        if column and re.search(rf"\b{re.escape(column.lower())}\b", where_clause):
            count += 1
    return count


def _extract_where_clause(sql: str) -> str:
    match = re.search(
        r"\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\bhaving\b|\bfetch\s+first\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return match.group(1)


def _estimated_select_column_count(sql: str) -> int:
    match = re.search(
        r"\bselect\b(.*?)\bfrom\b",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return 0
    segment = match.group(1).strip()
    if not segment:
        return 0
    return len([part for part in segment.split(",") if part.strip()])


def _metadata_table_columns(metadata_used: dict[str, Any]) -> dict[str, set[str]]:
    table_columns: dict[str, set[str]] = {}
    items = metadata_used.get("relevant_items")
    if not isinstance(items, list):
        return table_columns

    for item in items:
        if not isinstance(item, dict):
            continue
        table_name = _normalize_identifier(str(item.get("table", "")))
        if not table_name:
            continue
        column_names: set[str] = set()
        columns = item.get("columns")
        if isinstance(columns, list):
            for column in columns:
                if not isinstance(column, dict):
                    continue
                name = str(column.get("name", "")).strip()
                if name:
                    column_names.add(name.lower())
        if table_name not in table_columns:
            table_columns[table_name] = set()
        table_columns[table_name].update(column_names)

        bare = table_name.split(".")[-1]
        if bare not in table_columns:
            table_columns[bare] = set()
        table_columns[bare].update(column_names)
    return table_columns


def _extract_alias_map(
    sql: str,
    table_columns: dict[str, set[str]],
) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    matches = re.findall(
        r"\b(?:from|join)\s+([A-Za-z0-9_.$#\"]+)(?:\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_$#]*))?",
        sql,
        flags=re.IGNORECASE,
    )
    for table_token, alias_token in matches:
        normalized_table = _normalize_identifier(table_token)
        if not normalized_table:
            continue
        table_key = normalized_table
        if table_key not in table_columns:
            bare = table_key.split(".")[-1]
            if bare in table_columns:
                table_key = bare
            else:
                continue

        bare_key = table_key.split(".")[-1]
        alias_map[bare_key] = table_key

        alias = alias_token.strip().lower() if alias_token else ""
        if alias and alias not in _ALIAS_STOPWORDS:
            alias_map[alias] = table_key

    return alias_map


def _fallback_pick(evaluations: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not evaluations:
        return None
    eligible = [item for item in evaluations if not bool(item["hard_disqualified"])]
    pool = eligible if eligible else evaluations

    best = pool[0]
    best_score = float(best.get("fallback_score", float("-inf")))
    for item in pool[1:]:
        score = float(item.get("fallback_score", float("-inf")))
        if score > best_score:
            best = item
            best_score = score
    return best


def _request_mentions_aggregation(user_request: str) -> bool:
    lowered = user_request.lower()
    return any(token in lowered for token in _AGGREGATION_TOKENS)


def _request_mentions_detail(user_request: str) -> bool:
    lowered = user_request.lower()
    return any(token in lowered for token in _DETAIL_TOKENS)


def _tokenize(text: str) -> Counter[str]:
    return Counter(re.findall(r"[A-Za-z0-9_]{2,}", text.lower()))


def _token_overlap(left: Counter[str], right: Counter[str]) -> float:
    overlap = set(left.keys()) & set(right.keys())
    return float(sum(min(left[token], right[token]) for token in overlap))


def _extract_column_token(obligation: str) -> str | None:
    match = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", obligation)
    if not match:
        return None
    return match.group(1)


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


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


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
    if re.search(
        r"(=|<>|!=|<=|>=|<|>|\blike\b|\bbetween\b|\bin\b|\bis\b)",
        text,
        flags=re.IGNORECASE,
    ):
        return text

    token_match = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text)
    if not token_match:
        return text

    column = token_match.group(0).upper()
    bind_name = "report_period" if column.lower() == "report_period" else column.lower()
    return f"{column} = :{bind_name}"


def _normalize_identifier(value: str) -> str:
    identifier = value.strip().strip(",")
    if identifier.startswith('"') and identifier.endswith('"'):
        identifier = identifier[1:-1]
    return identifier.replace('"', "").lower()


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return output
