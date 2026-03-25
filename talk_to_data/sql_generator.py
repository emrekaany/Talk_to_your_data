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


class SQLCannotAnswerSuggestion(RuntimeError):
    """Raised when the LLM determines it cannot produce valid SQL for the request."""

    def __init__(self, reason: str, suggested_questions: list[str]):
        self.reason = reason
        self.suggested_questions = suggested_questions
        super().__init__(reason)


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
        system_prompt="You are a senior Oracle SQL engineer specializing in analytics queries. Return strict JSON only. Do not wrap JSON in markdown code fences.",
        prompt=prompt,
        temperature=0.1,
        max_tokens=3200,
    )
    _check_cannot_answer(raw)
    return _parse_candidates_json(raw)


def _build_sql_prompt(
    *,
    user_request: str,
    requirements: dict[str, Any],
    metadata: dict[str, Any],
    retry_context: dict[str, Any] | None,
    agent_rules: dict[str, Any] | None,
) -> str:
    metadata_text = _metadata_prompt_text(metadata, max_tables=15)
    oracle_constraints_text = _oracle_constraints_prompt_text(requirements)
    retry_text = _retry_context_prompt_text(retry_context)
    agent_rules_text = _agent_rules_prompt_text(agent_rules)
    return (
        f"Request (original):\n{user_request}\n\n"
        f"Requirements JSON:\n{compact_json(requirements)}\n\n"
        f"Metadata:\n{metadata_text}\n\n"
        f"Agent Rules:\n{agent_rules_text}\n\n"
        "Generation Rules:\n"
        "- Output strict JSON only. Do not wrap in markdown code fences.\n"
        '- JSON schema: {"candidates":[{"id":"option_1","sql":"...","rationale_short":"...","risk_notes":"..."},'
        '{"id":"option_2","sql":"...","rationale_short":"...","risk_notes":"..."},'
        '{"id":"option_3","sql":"...","rationale_short":"...","risk_notes":"..."}]}.\n'
        "- Generate three Oracle SQL candidates strictly from the provided metadata and request.\n"
        "- Use only tables, columns, and joins present in metadata.\n"
        "- Never invent, rename, infer, or alias a missing metadata column into existence.\n"
        "- Every referenced identifier must match a metadata table/column exactly, including in SELECT, JOIN, WHERE, GROUP BY, HAVING, and ORDER BY.\n"
        "- Treat the metadata table column lists as the complete allowlist; if a column is absent there, do not use it.\n"
        "\n"
        "CRITICAL ANTI-HALLUCINATION RULES:\n"
        "- Before writing each column reference, verify it appears in the metadata column list for that table.\n"
        "- If a column you expect is NOT listed in the metadata, it does NOT exist. Do NOT use it under any name or alias.\n"
        "- JOIN conditions must use ONLY column pairs explicitly documented in the metadata Joins section.\n"
        "- If a column has allowed_values listed in metadata, use ONLY those exact values in WHERE, HAVING, or CASE conditions for that column. Do not invent, abbreviate, or transliterate category names.\n"
        "\n"
        "- If you cannot create valid SQL that answers the request using ONLY the provided metadata columns, tables, and joins, "
        "return this alternative JSON instead:\n"
        '{"cannot_answer": true, "reason": "brief explanation of why", '
        '"suggested_questions": ["rephrased question 1 that CAN be answered with available metadata", '
        '"rephrased question 2"]}.\n'
        "- IMPORTANT: cannot_answer is a LAST RESORT. Before returning it, re-read the entire Metadata section above and verify every claim in your reason. "
        "If the table or column you believe is missing IS actually listed in the metadata, you MUST generate SQL instead.\n"
        "- When the user asks for a concept (e.g. 'acente adi', 'musteri', 'brans'), search ALL tables in metadata — not just the primary fact table. "
        "Dimension tables (ACE_ACENTE, MUS_MUSTERI, POL_URUN, POL_BRANS, etc.) are reachable via documented JoinDef paths.\n"
        "- Do not confuse 'column not in the fact table' with 'column not in metadata'. Use JoinDef paths to reach dimension columns.\n"
        "- If you return cannot_answer, each suggested_question MUST be a concrete data question that you have verified CAN be answered "
        "using ONLY the tables, columns, and joins listed in the Metadata section above. "
        "Do NOT suggest questions that reference tables or columns absent from metadata.\n"
        "- Only use the cannot_answer format when you are genuinely unable to answer; prefer generating valid SQL when possible.\n"
        "\n"
        "- Keep SELECT minimal: include only columns required for the final answer, plus columns strictly required for aggregate output ordering or grouping.\n"
        "- Do not project join-only, filter-only, helper, or intermediate calculation columns unless the user explicitly asked to see them.\n"
        "- Prefer aggregate expressions or ORDER BY aliases instead of exposing extra raw columns in SELECT.\n"
        "- Do not use SELECT *.\n"
        "- Do not include semicolons.\n"
        f"- Every SQL must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.\n"
        "- Do not return INVALID_REQUEST.\n"
        "- Keep each SQL under 30 lines.\n\n"
        f"Oracle Dialect & Execution Constraints:\n{oracle_constraints_text}\n\n"
        "Example output (minimal):\n"
        '{"candidates":['
        '{"id":"option_1","sql":"SELECT d.BOLGE_ADI, SUM(f.BRUT_PRIM_TL) AS TOPLAM_PRIM '
        "FROM AS_DWH.FACT_POL_POLICE_EK f "
        "INNER JOIN AS_DWH.POL_POLICE_OZET p ON f.POLICE_ID = p.POLICE_ID AND f.KAYNAK_SISTEM_ID = p.KAYNAK_SISTEM_ID "
        "INNER JOIN AS_DWH.GNL_TARIH t ON p.TANZIM_TARIH_ID = t.TARIH_ID "
        "WHERE TO_CHAR(t.TARIH, 'YYYY') = :year_value "
        'GROUP BY d.BOLGE_ADI ORDER BY TOPLAM_PRIM DESC FETCH FIRST 200 ROWS ONLY",'
        '"rationale_short":"Aggregates gross premium by region for the requested year",'
        '"risk_notes":"Assumes single calendar year scope"},'
        '{"id":"option_2","sql":"...","rationale_short":"...","risk_notes":"..."},'
        '{"id":"option_3","sql":"...","rationale_short":"...","risk_notes":"..."}]}\n\n'
        f"{retry_text}"
    )


def _retry_context_prompt_text(retry_context: dict[str, Any] | None) -> str:
    if not isinstance(retry_context, dict):
        return ""
    if not retry_context:
        return ""

    disqualify_reasons = _as_string_list(retry_context.get("disqualify_reasons"))[:10]
    blocked_patterns = _as_string_list(retry_context.get("blocked_sql_patterns"))[:8]
    rejected_columns = _as_string_list(retry_context.get("rejected_columns"))[:20]
    valid_columns_hint = _as_string_list(retry_context.get("valid_columns_hint"))[:30]
    if not disqualify_reasons and not blocked_patterns and not rejected_columns:
        return ""

    lines = [
        "Retry Guidance (Attempt 2):",
        "- This is a retry after candidate disqualification and/or judge failure.",
        "- CRITICAL: The previous attempt used columns that do NOT exist in metadata. Do NOT repeat this mistake.",
    ]
    if rejected_columns:
        lines.append(
            f"- REJECTED columns (do NOT use these, they do not exist): {', '.join(rejected_columns)}"
        )
    if valid_columns_hint:
        lines.append(
            f"- VALID columns you should use instead: {', '.join(valid_columns_hint)}"
        )
    if disqualify_reasons:
        lines.append(
            f"- Previously observed disqualify reasons: {', '.join(disqualify_reasons)}"
        )
    if blocked_patterns:
        lines.append(f"- Previously risky SQL patterns: {', '.join(blocked_patterns)}")
    lines.append("- Generate 3 alternatives that avoid these failures.")
    lines.append("- If you still cannot answer with valid columns, return the cannot_answer JSON format.")
    return "\n".join(lines) + "\n\n"


def _check_cannot_answer(raw: str) -> None:
    """Raise SQLCannotAnswerSuggestion if the LLM returned a cannot_answer response."""
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return
    if not isinstance(parsed, dict):
        return
    if not parsed.get("cannot_answer"):
        return
    reason = str(parsed.get("reason", "The request cannot be answered with available metadata.")).strip()
    suggestions = _as_string_list(parsed.get("suggested_questions"))
    raise SQLCannotAnswerSuggestion(reason=reason, suggested_questions=suggestions)


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


def _metadata_prompt_text(metadata: dict[str, Any], *, max_tables: int | None = None) -> str:
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
        if max_tables is not None:
            core_lower = {
                t.lower().strip()
                for t in _as_string_list(metadata.get("core_tables"))
            }
            prioritized = [
                item for item in relevant
                if float(item.get("score", 0)) > 0
                or str(item.get("table", "")).strip().lower() in core_lower
            ]
            if len(prioritized) > max_tables:
                core_items = [
                    i for i in prioritized
                    if str(i.get("table", "")).strip().lower() in core_lower
                ]
                non_core = [
                    i for i in prioritized
                    if str(i.get("table", "")).strip().lower() not in core_lower
                ]
                budget = max(0, max_tables - len(core_items))
                prioritized = core_items + non_core[:budget]
            relevant = prioritized
        for item in relevant:
            if not isinstance(item, dict):
                continue
            table = str(item.get("table", "")).strip()
            if not table:
                continue
            columns = _metadata_columns_text(item.get("columns"))
            if columns:
                lines.append(f"  - {table}({columns})")
                table_metadata = _filter_table_metadata(item.get("table_metadata"))
                if table_metadata:
                    for tm_line in _format_table_metadata(table_metadata):
                        lines.append(f"    {tm_line}")
            else:
                lines.append(f"  - {table}")

            joins = _as_string_list(item.get("joins"))
            if joins:
                lines.append(f"    Joins: {', '.join(joins)}")
            indexes = _as_string_list(item.get("indexes"))
            if indexes:
                lines.append(f"    Indexes: {', '.join(indexes)}")

    global_notes = _as_string_list(metadata.get("global_reporting_notes"))
    if global_notes:
        lines.append("- Global Reporting Notes:")
        for note in global_notes[:15]:
            lines.append(f"  - {_short_metadata_text(note, limit=500)}")

    mandatory_rules = _as_string_list(metadata.get("mandatory_rules"))
    if mandatory_rules:
        lines.append(f"- Mandatory rules: {', '.join(mandatory_rules)}")

    guardrails = _as_string_list(metadata.get("guardrails"))
    if guardrails:
        lines.append(f"- Guardrails: {', '.join(guardrails)}")

    return "\n".join(lines)


_TABLE_METADATA_ALLOWED_KEYS = frozenset({
    "description",
    "grain",
    "keywords",
    "business_notes",
    "performance_rules",
    "relationships",
    "mandatory_filters",
    "join_definitions",
})


def _filter_table_metadata(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {
        key: value
        for key, value in raw.items()
        if key in _TABLE_METADATA_ALLOWED_KEYS and value not in (None, "", [], {})
    }


def _format_table_metadata(tm: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    desc = _short_metadata_text(tm.get("description"), limit=800)
    if desc:
        lines.append(f"Description: {desc}")
    grain = _short_metadata_text(tm.get("grain"), limit=300)
    if grain:
        lines.append(f"Grain: {grain}")
    keywords = _as_string_list(tm.get("keywords"))
    if keywords:
        lines.append(f"Keywords: {', '.join(_short_metadata_text(k, limit=60) for k in keywords[:30])}")
    for note in _as_string_list(tm.get("business_notes"))[:15]:
        lines.append(f"Note: {_short_metadata_text(note, limit=500)}")
    for rule in _as_string_list(tm.get("performance_rules"))[:10]:
        lines.append(f"Perf: {_short_metadata_text(rule, limit=300)}")
    for flt in _as_string_list(tm.get("mandatory_filters"))[:10]:
        lines.append(f"Filter: {_short_metadata_text(flt, limit=200)}")
    join_defs = tm.get("join_definitions")
    if isinstance(join_defs, list) and join_defs:
        for jd in join_defs:
            if not isinstance(jd, dict):
                continue
            jtype = str(jd.get("join_type", "JOIN")).strip()
            wtable = str(jd.get("with_table", "")).strip()
            alias = str(jd.get("alias", "")).strip()
            on_clause = str(jd.get("on", "")).strip()
            semantic = str(jd.get("semantic", "")).strip()
            note = str(jd.get("note", "")).strip()
            if not wtable or not on_clause:
                continue
            parts = [jtype, wtable]
            if alias:
                parts.append(alias)
            parts.append(f"ON ({on_clause})")
            line = " ".join(parts)
            if semantic:
                line = f"{line} [{semantic}]"
            if note:
                line = f"{line} -- {note}"
            lines.append(f"JoinDef: {line}")
    else:
        rels = _as_string_list(tm.get("relationships"))
        if rels:
            lines.append(f"Relationships: {', '.join(_short_metadata_text(r, limit=200) for r in rels[:15])}")
    return lines


def _metadata_columns_text(columns: Any) -> str:
    if not isinstance(columns, list):
        return ""
    out: list[str] = []
    for col in columns:
        if not isinstance(col, dict):
            continue
        name = str(col.get("name", "")).strip()
        col_type = str(col.get("type", "")).strip()
        if not name:
            continue
        base = f"{name} {col_type}".strip()
        extras: list[str] = []

        description = _short_metadata_text(col.get("description"), limit=500)
        if description:
            extras.append(f"desc: {description}")

        semantic_type = _short_metadata_text(col.get("semantic_type"), limit=200)
        if semantic_type:
            extras.append(f"semantic_type: {semantic_type}")

        keywords = _as_string_list(col.get("keywords"))
        if keywords:
            extras.append(
                f"keywords: {', '.join(_short_metadata_text(item, limit=60) for item in keywords[:15])}"
            )

        properties = _as_string_list(col.get("properties"))
        if properties:
            extras.append(
                f"properties: {', '.join(_short_metadata_text(item, limit=100) for item in properties[:10])}"
            )

        select_expressions = _as_string_list(col.get("select_expressions"))
        if select_expressions:
            extras.append(
                f"select_expr: {', '.join(_short_metadata_text(item, limit=300) for item in select_expressions[:5])}"
            )

        allowed_values = _as_string_list(col.get("allowed_values"))
        if allowed_values:
            extras.append(
                f"allowed_values: {', '.join(_short_metadata_text(item, limit=60) for item in allowed_values[:100])}"
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


def _oracle_constraints_prompt_text(
    requirements: dict[str, Any],
) -> str:
    lines: list[str] = [
        "- Oracle SQL dialect is mandatory.",
        f"- Every query must include FETCH FIRST {DEFAULT_SQL_LIMIT} ROWS ONLY.",
        "- Use only metadata-backed tables, columns, and join paths.",
        "- SQL must be executable without semicolon terminators.",
        "- Every projected SELECT expression must be necessary for the requested output.",
        "- Keep filter and join support columns out of SELECT unless the request explicitly asks for them.",
        "- If ordering by a derived aggregate already selected, reuse that expression or alias instead of adding extra columns.",
        "- When using TO_CHAR on date columns, ALWAYS provide the format mask as the second argument: TO_CHAR(column, 'YYYY') for year, TO_CHAR(column, 'YYYYMM') for year-month, TO_CHAR(column, 'YYYYMMDD') for full date. Never write TO_CHAR(column) without a format mask.",
    ]

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
        ).content
    except LLMError as exc:
        raise SQLGenerationError(f"LLM request failed: {exc}") from exc


def generate_clarification_suggestions(
    user_request: str,
    metadata_used: dict[str, Any],
    validation_errors: list[str],
    llm_client: LLMClient | None = None,
) -> dict[str, Any]:
    """Generate suggested alternative questions when all SQL candidates fail.

    Returns dict with 'reason' and 'suggested_questions' keys.
    Every suggested question is validated against metadata before returning.
    """
    fallback_suggestions = _generate_fallback_suggestions(metadata_used)
    fallback_result = {
        "reason": "Mevcut metadata ile bu istek cevaplanamadi.",
        "suggested_questions": fallback_suggestions,
    }

    if llm_client is None:
        return fallback_result

    tables_summary = _tables_summary_for_suggestion(metadata_used)
    answerable_patterns = _build_answerable_patterns(metadata_used)
    errors_text = (
        "\n".join(f"- {e}" for e in validation_errors[:10])
        if validation_errors
        else "- Bilinmeyen dogrulama hatasi"
    )

    prompt = (
        "Kullanicinin veri sorusu mevcut veritabani metadata'si ile cevaplanamadi.\n\n"
        f"Kullanici istegi:\n{user_request}\n\n"
        f"Dogrulama hatalari (SQL adaylarinin basarisizlik nedenleri):\n{errors_text}\n\n"
        f"Mevcut tablolar, kolonlar ve join yollari:\n{tables_summary}\n\n"
        f"Cevaplanabilir sorgu desenleri (mevcut measure ve dimension'lar):\n{answerable_patterns}\n\n"
        "Gorev:\n"
        "1. Kisaca (Turkce) bu istegin neden cevaplanamadini acikla.\n"
        "2. Yukaridaki metadata'da listelenen tablo, kolon ve join yollarini kullanarak "
        "KESINLIKLE cevaplanabilecek 2-3 alternatif soru oner.\n"
        "3. KRITIK KURALLAR:\n"
        "   - Her onerilen soru SADECE yukaridaki metadata'da listelenen tablo ve kolonlari kullanmalidir.\n"
        "   - Metadata'da OLMAYAN tablo veya kolon referans eden soru ASLA onerme.\n"
        "   - Her oneride en az bir measure (toplam/sayim yapilabilecek kolon) ve uygun dimension (gruplama kolonu) kullan.\n"
        "   - Onerileri mumkun oldugunca kullanicinin orijinal niyetine yakin tut.\n"
        "   - Turkce yaz, dogal dilde sor (SQL yazma).\n"
        "   - Yukaridaki 'Cevaplanabilir sorgu desenleri' bolumundeki measure ve dimension isimlerini kullanarak onerileri olustur.\n\n"
        'Strict JSON formatinda don: {"reason": "...", "suggested_questions": ["...", "..."]}'
    )

    try:
        raw = llm_client.chat(
            "Sen yardimci bir veri analisti asistanisin. Tum cevaplarin Turkce olsun. Sadece gecerli JSON dondur.",
            prompt,
            temperature=0.2,
            max_tokens=800,
        ).content
    except LLMError:
        return fallback_result

    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return fallback_result

    if not isinstance(parsed, dict):
        return fallback_result

    reason = (
        str(parsed.get("reason", "")).strip()
        or "Mevcut metadata ile bu istek cevaplanamadi."
    )
    raw_suggestions = _as_string_list(parsed.get("suggested_questions"))[:5]

    # Use LLM suggestions if available, otherwise fall back to programmatic ones
    validated = raw_suggestions if raw_suggestions else fallback_suggestions

    return {
        "reason": reason,
        "suggested_questions": validated[:3],
    }


def _tables_summary_for_suggestion(metadata: dict[str, Any]) -> str:
    """Build a rich tables+columns+joins summary for the suggestion prompt."""
    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list) or not relevant:
        return "Metadata mevcut degil."
    lines: list[str] = []
    for item in relevant[:20]:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        if not table:
            continue
        # Columns with semantic type annotation
        columns = item.get("columns")
        col_parts: list[str] = []
        if isinstance(columns, list):
            for c in columns:
                if not isinstance(c, dict):
                    continue
                name = str(c.get("name", "")).strip()
                if not name:
                    continue
                sem = str(c.get("semantic_type", "")).strip()
                if sem:
                    col_parts.append(f"{name}[{sem}]")
                else:
                    col_parts.append(name)
        if col_parts:
            lines.append(f"{table}: {', '.join(col_parts)}")
        else:
            lines.append(table)
        # Table metadata context
        tm = item.get("table_metadata")
        if isinstance(tm, dict):
            kw = _as_string_list(tm.get("keywords"))
            if kw:
                lines.append(f"  Keywords: {', '.join(kw[:10])}")
            for note in _as_string_list(tm.get("business_notes"))[:3]:
                lines.append(f"  Note: {_short_metadata_text(note, limit=200)}")
            join_defs = tm.get("join_definitions")
            if isinstance(join_defs, list):
                for jd in join_defs[:5]:
                    if not isinstance(jd, dict):
                        continue
                    jtype = str(jd.get("join_type", "JOIN")).strip()
                    wtable = str(jd.get("with_table", "")).strip()
                    on_clause = str(jd.get("on", "")).strip()
                    if wtable and on_clause:
                        lines.append(f"  JoinDef: {jtype} {wtable} ON ({on_clause})")
            else:
                joins = _as_string_list(item.get("joins"))
                if joins:
                    lines.append(f"  Joins: {', '.join(joins[:5])}")
    return "\n".join(lines) if lines else "Metadata mevcut degil."


def _build_answerable_patterns(metadata: dict[str, Any]) -> str:
    """Auto-generate answerable query patterns from metadata structure."""
    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list):
        return "Desen mevcut degil."

    measures: list[str] = []
    dimensions: list[str] = []
    _KNOWN_MEASURES = frozenset({
        "BRUT_PRIM_TL", "VOP_TL", "POLICE_SAYISI",
    })
    for item in relevant:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        columns = item.get("columns")
        if not isinstance(columns, list):
            continue
        for c in columns:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name", "")).strip()
            sem = str(c.get("semantic_type", "")).strip().lower()
            if not name:
                continue
            if sem == "measure" or name in _KNOWN_MEASURES:
                measures.append(f"{table}.{name}")
            elif sem in ("dimensional", ""):
                kw = _as_string_list(c.get("keywords"))
                kw_str = f" ({', '.join(kw[:3])})" if kw else ""
                dimensions.append(f"{table}.{name}{kw_str}")

    lines: list[str] = []
    if measures:
        lines.append(f"Measure kolonlari (SUM/COUNT yapilabilir): {', '.join(measures[:10])}")
    if dimensions:
        lines.append(f"Dimension kolonlari (GROUP BY yapilabilir): {', '.join(dimensions[:15])}")
    if measures and dimensions:
        lines.append(
            "Ornek cevaplanabilir sorular: "
            "'Donem bazinda toplam brut prim uretimi', "
            "'Acente bazinda VOP toplami', "
            "'Urun koduna gore police sayisi'"
        )
    return "\n".join(lines) if lines else "Desen mevcut degil."


def _generate_fallback_suggestions(metadata: dict[str, Any]) -> list[str]:
    """Generate programmatic fallback suggestions from metadata structure."""
    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list):
        return []

    measures: list[tuple[str, str]] = []
    dimensions: list[tuple[str, str]] = []
    _KNOWN_MEASURES = frozenset({
        "BRUT_PRIM_TL", "VOP_TL", "POLICE_SAYISI",
    })
    _MEASURE_LABELS: dict[str, str] = {
        "BRUT_PRIM_TL": "brut prim",
        "VOP_TL": "vergi oncesi prim (VOP)",
        "POLICE_SAYISI": "police sayisi",
    }
    _DIM_LABELS: dict[str, str] = {
        "ACENTE_ADI": "acente",
        "ACENTE_KODU": "acente kodu",
        "ACENTE_KODU_NUMERIC": "acente",
        "URUN_ADI": "urun",
        "URUN_KODU": "urun kodu",
        "BRANS_ADI": "brans",
        "BRANS_KODU": "brans kodu",
        "IL_ADI": "il",
        "SATIS_KANALI": "satis kanali",
        "MUSTERI_ADI": "musteri",
        "POLICE_DURUMU_ADI": "police durumu",
        "SATIS_MUDURLUGU_ADI": "satis mudurlugu",
    }
    for item in relevant:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        columns = item.get("columns")
        if not isinstance(columns, list):
            continue
        for c in columns:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name", "")).strip()
            if not name:
                continue
            sem = str(c.get("semantic_type", "")).strip().lower()
            if sem == "measure" or name in _KNOWN_MEASURES:
                measures.append((table, name))
            elif name in _DIM_LABELS:
                dimensions.append((table, name))

    suggestions: list[str] = []
    used_combos: set[str] = set()
    for _m_table, m_col in measures[:3]:
        m_label = _MEASURE_LABELS.get(m_col, m_col)
        for _d_table, d_col in dimensions[:10]:
            d_label = _DIM_LABELS.get(d_col)
            if d_label is None:
                continue
            combo_key = f"{m_col}_{d_col}"
            if combo_key in used_combos:
                continue
            used_combos.add(combo_key)
            suggestions.append(f"{d_label} bazinda toplam {m_label} dagilimi")
            if len(suggestions) >= 3:
                break
        if len(suggestions) >= 3:
            break
    return suggestions[:3]
