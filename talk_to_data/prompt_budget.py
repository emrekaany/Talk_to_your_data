"""Prompt-budget-aware metadata serializers for LLM prompts."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PromptBudgetProfile:
    """Small profile describing how much metadata a prompt may receive."""

    name: str
    max_tables: int
    max_guardrails: int
    max_guardrail_chars: int
    include_candidate_tables: bool
    include_columns: bool = False
    max_columns_per_table: int = 0


JUDGE_PROMPT_PROFILE = PromptBudgetProfile(
    name="judge",
    max_tables=24,
    max_guardrails=10,
    max_guardrail_chars=140,
    include_candidate_tables=True,
)

EXPLAINER_PROMPT_PROFILE = PromptBudgetProfile(
    name="explainer",
    max_tables=8,
    max_guardrails=8,
    max_guardrail_chars=160,
    include_candidate_tables=False,
    include_columns=True,
    max_columns_per_table=12,
)

_IDENTIFIER_PATTERN = r'(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$#]*)'
_TABLE_TOKEN_PATTERN = rf"{_IDENTIFIER_PATTERN}(?:\s*\.\s*{_IDENTIFIER_PATTERN})?"
_TABLE_REF_PATTERN = re.compile(
    rf"\b(?:from|join)\s+({_TABLE_TOKEN_PATTERN})",
    flags=re.IGNORECASE,
)


def compact_prompt_json(data: Any) -> str:
    """Render compact JSON text for prompts."""
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))


def build_prompt_metadata_summary(
    metadata: dict[str, Any],
    *,
    profile: PromptBudgetProfile,
    candidate_sqls: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a prompt-safe metadata summary for a specific prompt budget."""
    summary: dict[str, Any] = {
        "dialect": metadata.get("dialect", "oracle sql"),
        "selected_tables": _selected_tables(metadata)[: profile.max_tables],
        "guardrail_notes": _guardrail_notes(
            metadata,
            limit=profile.max_guardrails,
            max_chars=profile.max_guardrail_chars,
        ),
    }

    if profile.include_columns:
        summary["table_columns"] = _table_columns(
            metadata,
            max_tables=profile.max_tables,
            max_columns_per_table=profile.max_columns_per_table,
        )

    if profile.include_candidate_tables:
        summary["candidate_tables"] = _candidate_table_map(candidate_sqls or {})

    return summary


def _selected_tables(metadata: dict[str, Any]) -> list[str]:
    tables: list[str] = []
    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list):
        return tables
    for item in relevant:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        if table:
            tables.append(table)
    return _dedupe(tables)


def _table_columns(
    metadata: dict[str, Any],
    *,
    max_tables: int,
    max_columns_per_table: int,
) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    relevant = metadata.get("relevant_items")
    if not isinstance(relevant, list):
        return tables

    for item in relevant:
        if len(tables) >= max_tables:
            break
        if not isinstance(item, dict):
            continue
        table = str(item.get("table", "")).strip()
        if not table:
            continue
        column_names: list[str] = []
        columns = item.get("columns")
        if isinstance(columns, list):
            for column in columns:
                if len(column_names) >= max_columns_per_table:
                    break
                if not isinstance(column, dict):
                    continue
                name = str(column.get("name", "")).strip()
                if name:
                    column_names.append(name)
        tables.append({"table": table, "columns": column_names})
    return tables


def _candidate_table_map(candidate_sqls: dict[str, str]) -> dict[str, list[str]]:
    output: dict[str, list[str]] = {}
    for candidate_id, sql in candidate_sqls.items():
        output[str(candidate_id)] = _extract_table_names(sql)
    return output


<<<<<<< ours
=======

>>>>>>> theirs
def _normalize_identifier_token(token: str) -> str:
    value = token.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1].replace('""', '"').lower()
    return value.lower()



def _extract_cte_names(sql: str) -> set[str]:
    """Return normalized CTE names defined in the leading WITH clause."""
    text = sql.lstrip()
    if not re.match(r"with\b", text, flags=re.IGNORECASE):
        return set()

    index = 4
    length = len(text)
    cte_names: set[str] = set()

    while index < length:
        index = _skip_whitespace(text, index)
        if re.match(r"recursive\b", text[index:], flags=re.IGNORECASE):
            index += len("recursive")
            index = _skip_whitespace(text, index)

        match = re.match(_IDENTIFIER_PATTERN, text[index:])
        if not match:
            break
        cte_name = match.group(0)
        cte_names.add(_normalize_identifier_token(cte_name))
        index += match.end()
        index = _skip_whitespace(text, index)

        if index < length and text[index] == '(':
            index = _consume_balanced(text, index, '(', ')')
            index = _skip_whitespace(text, index)

        as_match = re.match(r"as\b", text[index:], flags=re.IGNORECASE)
        if not as_match:
            break
        index += as_match.end()
        index = _skip_whitespace(text, index)

        if index >= length or text[index] != '(':
            break
        index = _consume_balanced(text, index, '(', ')')
        index = _skip_whitespace(text, index)

        if index < length and text[index] == ',':
            index += 1
            continue
        break

    return cte_names



def _extract_table_names(sql: str) -> list[str]:
    """Extract deduplicated physical table tokens referenced by FROM/JOIN clauses."""
    if not sql:
        return []

    cte_names = _extract_cte_names(sql)
    table_names: list[str] = []
    seen: set[str] = set()

    for match in _TABLE_REF_PATTERN.finditer(sql):
        token = re.sub(r"\s*\.\s*", ".", match.group(1).strip())
        normalized_parts = [
            _normalize_identifier_token(part)
            for part in re.split(r"\s*\.\s*", token)
            if part.strip()
        ]
        if not normalized_parts:
            continue
        bare_name = normalized_parts[-1]
        if bare_name in cte_names:
            continue
        seen_key = ".".join(normalized_parts)
        if seen_key in seen:
            continue
        seen.add(seen_key)
        table_names.append(token)
    return table_names


<<<<<<< ours
=======

>>>>>>> theirs
def _guardrail_notes(metadata: dict[str, Any], *, limit: int, max_chars: int) -> list[str]:
    notes = _as_string_list(metadata.get("guardrails")) + _as_string_list(
        metadata.get("mandatory_rules")
    )
    shortened: list[str] = []
    for note in notes:
        compact = _shorten_text(note, max_chars)
        if compact:
            shortened.append(compact)
        if len(shortened) >= limit:
            break
    return _dedupe(shortened)


<<<<<<< ours
=======

>>>>>>> theirs
def _shorten_text(text: str, max_chars: int) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if not compact:
        return ""
    if len(compact) <= max_chars:
        return compact
    return compact[: max_chars - 1].rstrip() + "…"


<<<<<<< ours
=======

>>>>>>> theirs
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


<<<<<<< ours
=======

>>>>>>> theirs
def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        items: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                items.append(text)
        return items
    text = str(value).strip()
    return [text] if text else []


<<<<<<< ours
=======

>>>>>>> theirs
def _skip_whitespace(text: str, index: int) -> int:
    while index < len(text) and text[index].isspace():
        index += 1
    return index



def _consume_balanced(text: str, index: int, opening: str, closing: str) -> int:
    if index >= len(text) or text[index] != opening:
        return index

    depth = 0
    while index < len(text):
        char = text[index]
        if char == '"':
            index = _consume_quoted_identifier(text, index)
            continue
        if char == "'":
            index = _consume_string_literal(text, index)
            continue
        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return index



def _consume_quoted_identifier(text: str, index: int) -> int:
    index += 1
    while index < len(text):
        if text[index] == '"':
            if index + 1 < len(text) and text[index + 1] == '"':
                index += 2
                continue
            return index + 1
        index += 1
    return index



def _consume_string_literal(text: str, index: int) -> int:
    index += 1
    while index < len(text):
        if text[index] == "'":
            if index + 1 < len(text) and text[index + 1] == "'":
                index += 2
                continue
            return index + 1
        index += 1
    return index



def _assert_cte_candidate_table_extraction() -> None:
    sql = '''
    WITH recent_policies AS (
        SELECT ps.POLICY_ID, ps.PRODUCT_CODE
        FROM POLICY_SUMMARY ps
        JOIN CORE."Policy Facts" pf ON pf.POLICY_ID = ps.POLICY_ID
    ), second_cte AS (
        SELECT rp.POLICY_ID
        FROM recent_policies rp
    )
    SELECT sc.POLICY_ID
    FROM second_cte sc
    JOIN POLICY_SUMMARY ps ON ps.POLICY_ID = sc.POLICY_ID
    FETCH FIRST 200 ROWS ONLY
    '''
    candidate_tables = _extract_table_names(sql)
    lowered = {name.lower().replace('"', '') for name in candidate_tables}
    assert "policy_summary" in lowered, candidate_tables
    assert "core.policy facts" in lowered, candidate_tables
    assert "recent_policies" not in lowered, candidate_tables
    assert "second_cte" not in lowered, candidate_tables


if __name__ == "__main__":
    _assert_cte_candidate_table_extraction()
    print("prompt_budget assertions passed")
