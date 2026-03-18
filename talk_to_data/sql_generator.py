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
    agent_rules: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    """
    Generate exactly 3 Oracle SQL candidates.

    Contract target: generate_sql_candidates(user_request, requirements, metadata) -> list[dict]
    """
    if llm_client is None:
        raise SQLGenerationError("LLM client is required for SQL generation.")

    candidates = _generate_with_llm(
        llm_client=llm_client,
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        retry_context=retry_context,
        agent_rules=agent_rules,
    )

    if len(candidates) != 3:
        raise SQLGenerationError(
            f"Model returned {len(candidates)} SQL candidate(s); expected exactly 3."
        )

    normalized: list[dict[str, str]] = []
    for idx, candidate in enumerate(candidates, start=1):
        sql = str(candidate.get("sql", "")).strip()
        if not sql:
            raise SQLGenerationError(f"Candidate option_{idx} SQL is empty.")
        candidate_id = str(candidate.get("id") or f"option_{idx}").strip() or f"option_{idx}"
        normalized.append(
            {
                "id": candidate_id,
                "sql": sql,
                "rationale_short": str(candidate.get("rationale_short", "")).strip()
                or "Alternative interpretation.",
                "risk_notes": str(candidate.get("risk_notes", "")).strip()
                or "Verify business definitions before execution.",
            }
        )
    return normalized


def sanity_check_sql(sql: str) -> tuple[bool, str]:
    """Light SQL safety and sanity checks."""
    clean = _clean_sql_for_validation(sql)
    if not clean:
        return False, "SQL is empty."

    if _has_semicolon(clean):
        return (
            False,
            "SQL must be a single statement without semicolon delimiters.",
        )

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

    if not _has_required_row_limit(clean):
        return (
            False,
            f"SQL must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.",
        )

    return True, "ok"


def _generate_with_llm(
    *,
    llm_client: LLMClient,
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    retry_context: dict[str, Any] | None,
    agent_rules: dict[str, Any] | None,
) -> list[dict[str, str]]:
    prompt = _build_sql_prompt(
        user_request=user_request,
        requirements=requirements,
        metadata=metadata,
        retry_context=retry_context,
        agent_rules=agent_rules,
    )
    raw = _call_llm(
        llm_client=llm_client,
        system_prompt="You are a senior Oracle SQL engineer. Return strict JSON only.",
        prompt=prompt,
        temperature=0.1,
        max_tokens=2200,
    )
    return _parse_candidates_json(raw)


def _build_sql_prompt(
    *,
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    retry_context: dict[str, Any] | None,
    agent_rules: dict[str, Any] | None,
) -> str:
    metadata_text = _metadata_prompt_text(metadata)
    sql_rule_text = _sql_rule_prompt_text(requirements, agent_rules)
    retry_text = _retry_context_prompt_text(retry_context)
    agent_rules_text = _agent_rules_prompt_text(agent_rules)
    return (
        "You are a senior SQL engineer. "
        "Generate three Oracle SQL candidates strictly from the provided metadata and request.\n\n"
        "Rules:\n"
        "- Output strict JSON only.\n"
        '- JSON schema: {"candidates":[{"id":"option_1","sql":"...","rationale_short":"...","risk_notes":"..."},'
        '{"id":"option_2","sql":"...","rationale_short":"...","risk_notes":"..."},'
        '{"id":"option_3","sql":"...","rationale_short":"...","risk_notes":"..."}]}.\n'
        "- Use only tables, columns, and joins present in metadata.\n"
        "- Never invent, rename, infer, or alias a missing metadata column into existence.\n"
        "- Every referenced identifier must match a metadata table/column exactly, including in SELECT, JOIN, WHERE, GROUP BY, HAVING, and ORDER BY.\n"
        "- Treat the metadata table column lists as the complete allowlist; if a column is absent there, do not use it.\n"
        "- Keep SELECT minimal: include only columns required for the final answer, plus columns strictly required for aggregate output ordering or grouping.\n"
        "- Do not project join-only, filter-only, helper, or intermediate calculation columns unless the user explicitly asked to see them.\n"
        "- Prefer aggregate expressions or ORDER BY aliases instead of exposing extra raw columns in SELECT.\n"
        "- Do not use SELECT *.\n"
        "- Do not include semicolons.\n"
        f"- Every SQL must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.\n"
        "- Do not return INVALID_REQUEST.\n\n"
        f"Agent Rules:\n{agent_rules_text}\n\n"
        f"Metadata:\n{metadata_text}\n\n"
        f"Request (original):\n{user_request}\n\n"
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
        lines.append(
            f"- Previously observed disqualify reasons: {', '.join(disqualify_reasons)}"
        )
    if blocked_patterns:
        lines.append(f"- Previously risky SQL patterns: {', '.join(blocked_patterns)}")
    lines.append("- Generate 3 alternatives that avoid these failures.")
    return "\n".join(lines) + "\n\n"


def _parse_candidates_json(raw: str) -> list[dict[str, str]]:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return _parse_candidates_from_value(parsed)


def _parse_candidates_from_value(parsed: Any) -> list[dict[str, str]]:
    if isinstance(parsed, list):
        return _candidates_from_list(parsed)

    if not isinstance(parsed, dict):
        return []

    items = parsed.get("candidates")
    if not isinstance(items, list):
        items = parsed.get("queries")
    if not isinstance(items, list):
        sql_1 = parsed.get("sql_1")
        sql_2 = parsed.get("sql_2")
        sql_3 = parsed.get("sql_3")
        return _candidates_from_list([sql_1, sql_2, sql_3])

    return _candidates_from_list(items)


def _candidates_from_list(items: list[Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for idx, item in enumerate(items, start=1):
        if idx > 3:
            break
        candidate = _candidate_from_value(item, idx)
        if candidate is None:
            continue
        candidates.append(candidate)
    return candidates


def _candidate_from_value(item: Any, idx: int) -> dict[str, str] | None:
    if isinstance(item, str):
        sql = item.strip()
        if not sql:
            return None
        return {
            "id": f"option_{idx}",
            "sql": sql,
            "rationale_short": "",
            "risk_notes": "",
        }

    if not isinstance(item, dict):
        return None

    sql = str(item.get("sql", "")).strip()
    if not sql:
        return None
    return {
        "id": str(item.get("id") or f"option_{idx}"),
        "sql": sql,
        "rationale_short": str(item.get("rationale_short", "")).strip(),
        "risk_notes": str(item.get("risk_notes", "")).strip(),
    }


def _clean_sql_for_validation(sql: str) -> str:
    return _strip_fence(str(sql)).strip()


def _has_required_row_limit(sql: str) -> bool:
    return bool(
        re.search(
            rf"\bfetch\s+first\s+{DEFAULT_SQL_LIMIT}\s+rows\s+only\b",
            sql,
            flags=re.IGNORECASE,
        )
    )


def _has_semicolon(sql: str) -> bool:
    in_single = False
    in_double = False
    for char in sql:
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == ";" and not in_single and not in_double:
            return True
    return False


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


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
    lines.append("- Tables (complete allowlist; only these columns may be referenced):")

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


def _sql_rule_prompt_text(
    requirements: dict[str, Any],
    agent_rules: dict[str, Any] | None,
) -> str:
    lines: list[str] = [
        "- Oracle SQL dialect is mandatory.",
        f"- Every query must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.",
        "- Use only metadata-backed tables, columns, and join paths.",
        "- SQL must be executable without semicolon terminators.",
        "- Every projected SELECT expression must be necessary for the requested output.",
        "- Keep filter and join support columns out of SELECT unless the request explicitly asks for them.",
        "- If ordering by a derived aggregate already selected, reuse that expression or alias instead of adding extra columns.",
    ]

    for rule in _as_string_list((agent_rules or {}).get("sql_prompt_rules")):
        lines.append(f"- Agent rule: {rule}")
    for rule in _as_string_list((agent_rules or {}).get("time_expression_guidance")):
        lines.append(f"- Time guidance: {rule}")

    time_granularity = str(requirements.get("time_granularity", "")).strip()
    if time_granularity:
        lines.append(f"- Runtime hint time_granularity: {time_granularity}")
    time_value = str(requirements.get("time_value", "")).strip()
    if time_value:
        lines.append(f"- Runtime hint time_value: {time_value}")

    time_range = requirements.get("time_range")
    if isinstance(time_range, dict):
        start = str(time_range.get("start", "")).strip()
        end = str(time_range.get("end", "")).strip()
        if start:
            lines.append(f"- Runtime hint start_date: {start}")
        if end:
            lines.append(f"- Runtime hint end_date: {end}")

    return "\n".join(lines)


def _agent_rules_prompt_text(agent_rules: dict[str, Any] | None) -> str:
    if not isinstance(agent_rules, dict):
        return "- No agent-specific prompt rules provided."
    lines: list[str] = []
    agent_id = str(agent_rules.get("agent_id", "")).strip()
    if agent_id:
        lines.append(f"- Agent ID: {agent_id}")
    sql_rules = _as_string_list(agent_rules.get("sql_prompt_rules"))
    if sql_rules:
        lines.append("- SQL Prompt Rules:")
        for rule in sql_rules:
            lines.append(f"  - {rule}")
    time_rules = _as_string_list(agent_rules.get("time_expression_guidance"))
    if time_rules:
        lines.append("- Time Expression Guidance:")
        for rule in time_rules:
            lines.append(f"  - {rule}")
    if not lines:
        lines.append("- No agent-specific prompt rules provided.")
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
