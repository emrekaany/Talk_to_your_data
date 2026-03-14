"""Shared SQL validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


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


@dataclass(frozen=True)
class UnknownAliasColumnViolation:
    """Unknown alias.column reference mapped to an expected metadata table."""

    alias: str
    column: str
    expected_table: str

    @property
    def reference(self) -> str:
        return f"{self.alias}.{self.column}"


@dataclass
class _TableColumns:
    expected_table: str
    columns: set[str]


def find_unknown_alias_column_violations(
    sql: str,
    metadata_used: dict[str, Any],
) -> list[UnknownAliasColumnViolation]:
    """Return alias.column references that are not present in metadata table columns."""
    table_columns = _metadata_table_columns(metadata_used)
    if not table_columns:
        return []

    alias_map = _extract_alias_map(sql, table_columns)
    if not alias_map:
        return []

    violations: list[UnknownAliasColumnViolation] = []
    seen: set[tuple[str, str, str]] = set()
    for alias, column in re.findall(
        r"\b([A-Za-z_][A-Za-z0-9_$#]*)\.([A-Za-z_][A-Za-z0-9_$#]*)\b",
        sql,
        flags=re.IGNORECASE,
    ):
        alias_key = alias.lower()
        table_key = alias_map.get(alias_key)
        if not table_key:
            continue

        table_info = table_columns.get(table_key)
        if table_info is None or not table_info.columns:
            continue
        if column.lower() in table_info.columns:
            continue

        signature = (alias_key, column.lower(), table_key)
        if signature in seen:
            continue
        seen.add(signature)
        violations.append(
            UnknownAliasColumnViolation(
                alias=alias,
                column=column,
                expected_table=table_info.expected_table,
            )
        )
    return violations


def _metadata_table_columns(metadata_used: dict[str, Any]) -> dict[str, _TableColumns]:
    table_columns: dict[str, _TableColumns] = {}
    items = metadata_used.get("relevant_items")
    if not isinstance(items, list):
        return table_columns

    for item in items:
        if not isinstance(item, dict):
            continue
        table_name = str(item.get("table", "")).strip()
        normalized_table = _normalize_identifier(table_name)
        if not normalized_table:
            continue

        display_name = table_name or normalized_table
        column_names = _extract_column_names(item.get("columns"))

        _merge_table_columns(
            table_columns=table_columns,
            table_key=normalized_table,
            display_name=display_name,
            columns=column_names,
        )
        _merge_table_columns(
            table_columns=table_columns,
            table_key=normalized_table.split(".")[-1],
            display_name=display_name,
            columns=column_names,
        )
    return table_columns


def _merge_table_columns(
    *,
    table_columns: dict[str, _TableColumns],
    table_key: str,
    display_name: str,
    columns: set[str],
) -> None:
    existing = table_columns.get(table_key)
    if existing is None:
        table_columns[table_key] = _TableColumns(
            expected_table=display_name,
            columns=set(columns),
        )
        return

    if "." in display_name and "." not in existing.expected_table:
        existing.expected_table = display_name
    existing.columns.update(columns)


def _extract_column_names(raw_columns: Any) -> set[str]:
    column_names: set[str] = set()
    if not isinstance(raw_columns, list):
        return column_names
    for column in raw_columns:
        if isinstance(column, dict):
            name = str(column.get("name", "")).strip()
        else:
            name = str(column).strip()
        if name:
            column_names.add(name.lower())
    return column_names


def _extract_alias_map(
    sql: str,
    table_columns: dict[str, _TableColumns],
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


def _normalize_identifier(value: str) -> str:
    identifier = value.strip().strip(",")
    if identifier.startswith('"') and identifier.endswith('"'):
        identifier = identifier[1:-1]
    return identifier.replace('"', "").lower()
