"""Human-readable SQL candidate descriptions."""

from __future__ import annotations

import json
import re
from typing import Any

from .llm_client import LLMClient, LLMError
from .prompt_budget import EXPLAINER_PROMPT_PROFILE, build_prompt_metadata_summary


def describe_sql_candidate(
    candidate: dict[str, Any],
    metadata: dict[str, Any],
    llm_client: LLMClient | None = None,
) -> str:
    """Produce plain-language explanation for a SQL candidate."""
    sql = str(candidate.get("sql", "")).strip()
    if not sql:
        return "No SQL text was provided."

    llm_description = _describe_with_llm(sql, metadata, llm_client=llm_client)
    if llm_description:
        return llm_description

    tables = _extract_tables(sql, metadata)
    filters = _extract_where_clause(sql)
    grouping = _extract_group_by_clause(sql)
    select_cols = _extract_select_columns(sql)
    assumptions = _infer_assumptions(sql, metadata)

    lines = [
        "Question answered: Alternative SQL interpretation of the user request.",
        f"Tables used: {', '.join(tables) if tables else 'Not clearly identifiable from SQL text.'}",
        f"Filters: {filters or 'No explicit filter found.'}",
        f"Grouping/measures: {grouping or 'No GROUP BY clause (row-level or pre-aggregated output).'}",
        f"Expected output columns: {', '.join(select_cols) if select_cols else 'Unable to parse selected columns.'}",
        f"Assumptions: {assumptions}",
    ]
    return "\n".join(lines)


def describe_sql_candidates(
    candidates: list[dict[str, Any]],
    metadata: dict[str, Any],
    llm_client: LLMClient | None = None,
    *,
    llm_enabled: bool = True,
    batch_enabled: bool = True,
) -> list[str]:
    """Produce descriptions for all candidates with one batch prompt when enabled."""
    if not candidates:
        return []

    if llm_enabled and llm_client is not None and batch_enabled:
        batch_descriptions = _describe_batch_with_llm(
            candidates,
            metadata,
            llm_client=llm_client,
        )
        if batch_descriptions is not None and len(batch_descriptions) == len(candidates):
            return batch_descriptions

    return [
        describe_sql_candidate(
            candidate,
            metadata,
            llm_client=llm_client if llm_enabled and not batch_enabled else None,
        )
        for candidate in candidates
    ]


def _describe_with_llm(
    sql: str,
    metadata: dict[str, Any],
    *,
    llm_client: LLMClient | None,
) -> str | None:
    if llm_client is None:
        return None

    prompt = (
        "Explain the SQL query in plain business language.\n"
        "Keep it concise and practical.\n"
        "Use this output template exactly as plain text lines:\n"
        "Question answered: ...\n"
        "Tables used: ...\n"
        "Filters: ...\n"
        "Grouping/measures: ...\n"
        "Expected output columns: ...\n"
        "Assumptions: ...\n\n"
        f"SQL:\n{sql}\n\n"
        f"Relevant metadata summary:\n{_metadata_summary_for_prompt(metadata)}"
    )
    try:
        output = llm_client.chat(
            "You are a senior analytics engineer who explains SQL clearly.",
            prompt,
            temperature=0.0,
            max_tokens=500,
        )
    except LLMError:
        return None

    text = output.strip()
    if not text:
        return None
    if "Question answered:" not in text:
        return None
    return _strip_fence(text)


def _describe_batch_with_llm(
    candidates: list[dict[str, Any]],
    metadata: dict[str, Any],
    *,
    llm_client: LLMClient | None,
) -> list[str] | None:
    if llm_client is None:
        return None

    candidate_payload = [
        {
            "id": str(candidate.get("id", f"option_{index}")).strip() or f"option_{index}",
            "sql": str(candidate.get("sql", "")).strip(),
        }
        for index, candidate in enumerate(candidates, start=1)
    ]

    prompt = (
        "Explain each SQL candidate in plain business language.\n"
        "Return strict JSON only with schema:\n"
        '{"descriptions":[{"id":"option_1","description":"..."},{"id":"option_2","description":"..."},{"id":"option_3","description":"..."}]}\n'
        "Each description must use this exact line template inside the string:\n"
        "Question answered: ...\\n"
        "Tables used: ...\\n"
        "Filters: ...\\n"
        "Grouping/measures: ...\\n"
        "Expected output columns: ...\\n"
        "Assumptions: ...\n\n"
        f"Candidates:\n{json.dumps(candidate_payload, ensure_ascii=False, indent=2)}\n\n"
        f"Relevant metadata summary:\n{_metadata_summary_for_prompt(metadata)}"
    )
    try:
        output = llm_client.chat(
            "You are a senior analytics engineer who explains SQL clearly.",
            prompt,
            temperature=0.0,
            max_tokens=1400,
        )
    except LLMError:
        return None

    parsed = _parse_batch_descriptions(output)
    if parsed is None:
        return None

    descriptions_by_id = {item["id"]: item["description"] for item in parsed}
    descriptions: list[str] = []
    for index, candidate in enumerate(candidates, start=1):
        candidate_id = str(candidate.get("id", f"option_{index}")).strip() or f"option_{index}"
        description = descriptions_by_id.get(candidate_id)
        if not description:
            return None
        descriptions.append(description)
    return descriptions


def _metadata_summary_for_prompt(metadata: dict[str, Any]) -> str:
    summary = build_prompt_metadata_summary(
        metadata,
        profile=EXPLAINER_PROMPT_PROFILE,
    )
    return json.dumps(summary, ensure_ascii=False, indent=2)


def _extract_tables(sql: str, metadata: dict[str, Any]) -> list[str]:
    lower_sql = sql.lower()
    candidates: list[str] = []
    relevant = metadata.get("relevant_items")
    if isinstance(relevant, list):
        for item in relevant:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table", "")).strip()
            if table and table.lower() in lower_sql:
                candidates.append(table)

    if candidates:
        return _dedupe(candidates)

    matches = re.findall(
        r"\bfrom\s+([a-zA-Z0-9_.]+)|\bjoin\s+([a-zA-Z0-9_.]+)",
        sql,
        flags=re.IGNORECASE,
    )
    extracted = []
    for from_match, join_match in matches:
        table = from_match or join_match
        table = table.strip()
        if table:
            extracted.append(table)
    return _dedupe(extracted)


def _extract_where_clause(sql: str) -> str:
    match = re.search(
        r"\bwhere\b(.*?)(\bgroup\s+by\b|\border\s+by\b|\bhaving\b|\bfetch\s+first\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return _normalize_space(match.group(1))


def _extract_group_by_clause(sql: str) -> str:
    group_match = re.search(
        r"\bgroup\s+by\b(.*?)(\border\s+by\b|\bhaving\b|\bfetch\s+first\b|$)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not group_match:
        return ""
    return f"GROUP BY {_normalize_space(group_match.group(1))}"


def _extract_select_columns(sql: str) -> list[str]:
    match = re.search(
        r"\bselect\b(.*?)\bfrom\b",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    clause = match.group(1)
    cols = [part.strip() for part in clause.split(",")]
    cols = [re.sub(r"\s+", " ", col) for col in cols if col]
    return cols[:12]


def _infer_assumptions(sql: str, metadata: dict[str, Any]) -> str:
    assumptions: list[str] = []
    if ":report_period" in sql.lower():
        assumptions.append("A report period value must be provided at runtime.")
    if ":n" in sql.lower():
        assumptions.append("Row limit uses bind variable :n.")
    mandatory = metadata.get("mandatory_rules")
    if isinstance(mandatory, list) and mandatory:
        assumptions.append("Mandatory metadata guardrails were applied.")
    if not assumptions:
        assumptions.append("Assumes selected columns and joins match business intent.")
    return " ".join(assumptions)


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


def _parse_batch_descriptions(raw: str) -> list[dict[str, str]] | None:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict):
        return None
    items = parsed.get("descriptions")
    if not isinstance(items, list):
        return None

    descriptions: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            return None
        candidate_id = str(item.get("id", "")).strip()
        description = _strip_fence(str(item.get("description", "")).strip())
        if not candidate_id or not description:
            return None
        if "Question answered:" not in description:
            return None
        descriptions.append({"id": candidate_id, "description": description})
    return descriptions


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
