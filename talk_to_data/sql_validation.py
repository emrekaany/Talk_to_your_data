"""Shared SQL validation helpers."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


_IDENTIFIER_PATTERN = r'(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_$#]*)'
_TABLE_TOKEN_PATTERN = rf"{_IDENTIFIER_PATTERN}(?:\s*\.\s*{_IDENTIFIER_PATTERN})?"
_TABLE_REF_PATTERN = re.compile(
    rf"\b(?:from|join)\s+({_TABLE_TOKEN_PATTERN})(?:\s+(?:as\s+)?({_IDENTIFIER_PATTERN}))?",
    flags=re.IGNORECASE,
)
_QUALIFIED_REF_PATTERN = re.compile(
    rf"({_IDENTIFIER_PATTERN})\s*\.\s*({_IDENTIFIER_PATTERN})(?:\s*\.\s*({_IDENTIFIER_PATTERN}))?",
    flags=re.IGNORECASE,
)
_ALIAS_STOPWORDS = {
    "on",
    "where",
    "group",
    "order",
    "fetch",
    "having",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "join",
    "union",
    "minus",
    "intersect",
    "connect",
    "start",
    "pivot",
    "unpivot",
    "model",
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


@dataclass(frozen=True)
class AmbiguousTableReferenceViolation:
    """Bare table reference maps to multiple schema-qualified tables."""

    reference: str
    candidate_tables: tuple[str, ...]
    alias: str | None = None

    @property
    def scoped_reference(self) -> str:
        if not self.alias:
            return self.reference
        return f"{self.reference} AS {self.alias}"


@dataclass(frozen=True)
class UnresolvedTableReferenceViolation:
    """Table.column reference where the table exists in metadata but is not joined."""

    alias: str
    column: str
    known_table: str

    @property
    def reference(self) -> str:
        return f"{self.alias}.{self.column}"


@dataclass(frozen=True)
class SQLColumnValidationResult:
    """Result of alias/table/column validation parsing."""

    unknown_columns: tuple[UnknownAliasColumnViolation, ...]
    ambiguous_table_references: tuple[AmbiguousTableReferenceViolation, ...]
    unresolved_table_references: tuple[UnresolvedTableReferenceViolation, ...] = ()


@dataclass(frozen=True)
class _AliasResolution:
    alias_map: dict[str, str]
    ambiguous_table_references: tuple[AmbiguousTableReferenceViolation, ...]


def build_validation_catalog(documents: list[dict[str, Any]]) -> dict[str, Any]:
    """Build full table-column catalog from raw metadata documents."""
    tables: dict[str, dict[str, Any]] = {}
    bare_to_full: dict[str, set[str]] = {}

    for doc in documents:
        if not isinstance(doc, dict):
            continue
        table_display = _table_name_from_document(doc)
        table_key = _normalize_qualified_identifier(table_display)
        if not table_key:
            continue
        columns = _extract_column_names(
            doc.get("columns"),
            table_key=table_key,
            raw_joins=doc.get("joins"),
            raw_relationships=doc.get("relationships"),
        )
        _merge_catalog_table(
            tables=tables,
            bare_to_full=bare_to_full,
            table_key=table_key,
            table_display=table_display,
            columns=columns,
        )

    return _serialize_catalog(tables, bare_to_full)


def analyze_sql_column_validation(
    sql: str,
    metadata_used: dict[str, Any],
    validation_catalog: dict[str, Any] | None = None,
) -> SQLColumnValidationResult:
    """Analyze alias/column references and table ambiguity for a SQL statement."""
    catalog = _effective_catalog(metadata_used, validation_catalog)
    alias_resolution = _resolve_aliases(sql, catalog)
    unknown_columns = _find_unknown_columns(sql, alias_resolution.alias_map, catalog)
    unresolved_refs = _find_unresolved_table_refs(sql, alias_resolution.alias_map, catalog)
    return SQLColumnValidationResult(
        unknown_columns=tuple(unknown_columns),
        ambiguous_table_references=alias_resolution.ambiguous_table_references,
        unresolved_table_references=tuple(unresolved_refs),
    )


def find_unknown_alias_column_violations(
    sql: str,
    metadata_used: dict[str, Any],
    validation_catalog: dict[str, Any] | None = None,
) -> list[UnknownAliasColumnViolation]:
    """Return alias.column references that are not present in metadata table columns."""
    result = analyze_sql_column_validation(
        sql=sql,
        metadata_used=metadata_used,
        validation_catalog=validation_catalog,
    )
    return list(result.unknown_columns)


def find_ambiguous_table_reference_violations(
    sql: str,
    metadata_used: dict[str, Any],
    validation_catalog: dict[str, Any] | None = None,
) -> list[AmbiguousTableReferenceViolation]:
    """Return ambiguous bare table references found in FROM/JOIN clauses."""
    result = analyze_sql_column_validation(
        sql=sql,
        metadata_used=metadata_used,
        validation_catalog=validation_catalog,
    )
    return list(result.ambiguous_table_references)


def _effective_catalog(
    metadata_used: dict[str, Any],
    validation_catalog: dict[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(validation_catalog, dict):
        normalized = _normalize_serialized_catalog(validation_catalog)
        if normalized["tables"]:
            return normalized
    return _build_catalog_from_metadata_used(metadata_used)


def _build_catalog_from_metadata_used(metadata_used: dict[str, Any]) -> dict[str, Any]:
    tables: dict[str, dict[str, Any]] = {}
    bare_to_full: dict[str, set[str]] = {}
    items = metadata_used.get("relevant_items")
    if not isinstance(items, list):
        return _serialize_catalog(tables, bare_to_full)

    for item in items:
        if not isinstance(item, dict):
            continue
        table_display = str(item.get("table", "")).strip()
        table_key = _normalize_qualified_identifier(table_display)
        if not table_key:
            continue
        columns = _extract_column_names(
            item.get("columns"),
            table_key=table_key,
            raw_joins=item.get("joins"),
            raw_relationships=item.get("relationships"),
        )
        _merge_catalog_table(
            tables=tables,
            bare_to_full=bare_to_full,
            table_key=table_key,
            table_display=table_display,
            columns=columns,
        )

    return _serialize_catalog(tables, bare_to_full)


def _merge_catalog_table(
    *,
    tables: dict[str, dict[str, Any]],
    bare_to_full: dict[str, set[str]],
    table_key: str,
    table_display: str,
    columns: set[str],
) -> None:
    existing = tables.get(table_key)
    if existing is None:
        existing_columns: set[str] = set()
        existing = {
            "display_name": table_display or table_key,
            "columns": existing_columns,
        }
        tables[table_key] = existing
    else:
        existing_columns = existing.get("columns")
        if not isinstance(existing_columns, set):
            existing_columns = set(_as_string_list(existing_columns))
            existing["columns"] = existing_columns
        if "." in table_display and "." not in str(existing.get("display_name", "")):
            existing["display_name"] = table_display

    existing_columns.update(columns)
    bare_key = table_key.split(".")[-1]
    bare_to_full.setdefault(bare_key, set()).add(table_key)


def _serialize_catalog(
    tables: dict[str, dict[str, Any]],
    bare_to_full: dict[str, set[str]],
) -> dict[str, Any]:
    serialized_tables: dict[str, dict[str, Any]] = {}
    for table_key, payload in tables.items():
        raw_columns = payload.get("columns")
        if isinstance(raw_columns, set):
            columns = sorted(raw_columns)
        else:
            columns = sorted({value.lower() for value in _as_string_list(raw_columns)})
        serialized_tables[table_key] = {
            "display_name": str(payload.get("display_name", table_key)).strip() or table_key,
            "columns": columns,
        }
    return {
        "tables": serialized_tables,
        "bare_to_full": {
            key: sorted(values)
            for key, values in bare_to_full.items()
            if values
        },
    }


def _normalize_serialized_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    tables_out: dict[str, dict[str, Any]] = {}
    bare_to_full_out: dict[str, list[str]] = {}

    raw_tables = catalog.get("tables")
    if isinstance(raw_tables, dict):
        for raw_key, raw_payload in raw_tables.items():
            table_key = _normalize_qualified_identifier(str(raw_key))
            if not table_key:
                continue
            if isinstance(raw_payload, dict):
                display_name = str(raw_payload.get("display_name", raw_key)).strip() or str(raw_key)
                columns = {
                    _normalize_identifier_token(column)
                    for column in _as_string_list(raw_payload.get("columns"))
                }
            else:
                display_name = str(raw_key)
                columns = {
                    _normalize_identifier_token(column)
                    for column in _as_string_list(raw_payload)
                }
            tables_out[table_key] = {
                "display_name": display_name,
                "columns": sorted(column for column in columns if column),
            }

    raw_bare = catalog.get("bare_to_full")
    if isinstance(raw_bare, dict):
        for raw_key, raw_values in raw_bare.items():
            bare_key = _normalize_identifier_token(str(raw_key))
            if not bare_key:
                continue
            normalized_values = sorted(
                {
                    _normalize_qualified_identifier(value)
                    for value in _as_string_list(raw_values)
                }
            )
            normalized_values = [value for value in normalized_values if value]
            if normalized_values:
                bare_to_full_out[bare_key] = normalized_values

    for table_key in tables_out:
        bare_key = table_key.split(".")[-1]
        values = set(bare_to_full_out.get(bare_key, []))
        values.add(table_key)
        bare_to_full_out[bare_key] = sorted(values)

    return {
        "tables": tables_out,
        "bare_to_full": bare_to_full_out,
    }


def _resolve_aliases(sql: str, catalog: dict[str, Any]) -> _AliasResolution:
    alias_map: dict[str, str] = {}
    ambiguous: list[AmbiguousTableReferenceViolation] = []
    seen_ambiguous: set[tuple[str, str | None, tuple[str, ...]]] = set()

    for match in _TABLE_REF_PATTERN.finditer(sql):
        table_token = str(match.group(1) or "").strip()
        alias_token = str(match.group(2) or "").strip()
        table_key, candidates = _resolve_table_token(table_token, catalog)
        if candidates:
            signature = (
                _normalize_identifier_token(table_token),
                _normalize_identifier_token(alias_token) if alias_token else None,
                candidates,
            )
            if signature not in seen_ambiguous:
                seen_ambiguous.add(signature)
                ambiguous.append(
                    AmbiguousTableReferenceViolation(
                        reference=table_token,
                        candidate_tables=candidates,
                        alias=alias_token or None,
                    )
                )
            continue
        if not table_key:
            continue

        bare_key = table_key.split(".")[-1]
        alias_map[bare_key] = table_key

        alias_key = _normalize_identifier_token(alias_token) if alias_token else ""
        if alias_key and alias_key not in _ALIAS_STOPWORDS:
            alias_map[alias_key] = table_key

    return _AliasResolution(
        alias_map=alias_map,
        ambiguous_table_references=tuple(ambiguous),
    )


def _resolve_table_token(
    table_token: str,
    catalog: dict[str, Any],
) -> tuple[str | None, tuple[str, ...]]:
    tables = catalog.get("tables")
    bare_to_full = catalog.get("bare_to_full")
    if not isinstance(tables, dict) or not isinstance(bare_to_full, dict):
        return None, ()

    parts = _split_identifier_parts(table_token)
    if not parts:
        return None, ()

    if len(parts) >= 2:
        full_key = ".".join(parts[-2:])
        if full_key in tables:
            return full_key, ()
        return None, ()

    bare_key = parts[0]
    candidate_keys = list(_as_string_list(bare_to_full.get(bare_key)))
    if bare_key in tables and bare_key not in candidate_keys:
        candidate_keys.append(bare_key)
    candidate_keys = sorted({key for key in candidate_keys if key in tables})
    if len(candidate_keys) == 1:
        return candidate_keys[0], ()
    if len(candidate_keys) > 1:
        candidates = tuple(
            str(tables[key].get("display_name", key)).strip() or key
            for key in candidate_keys
        )
        return None, candidates
    if bare_key in tables:
        return bare_key, ()
    return None, ()


_JOIN_ON_PATTERN = re.compile(
    r"\bON\s*\(\s*"
    rf"({_IDENTIFIER_PATTERN})\s*\.\s*({_IDENTIFIER_PATTERN})"
    r"\s*=\s*"
    rf"({_IDENTIFIER_PATTERN})\s*\.\s*({_IDENTIFIER_PATTERN})"
    r"\s*\)",
    flags=re.IGNORECASE,
)


def _extract_join_on_refs(
    sql: str,
    alias_map: dict[str, str],
    tables: dict[str, Any],
) -> set[tuple[str, str]]:
    """Return (alias_key, column_key) pairs used in JOIN ON clauses
    where both sides resolve to known metadata tables."""
    relaxed: set[tuple[str, str]] = set()
    for m in _JOIN_ON_PATTERN.finditer(sql):
        left_alias = _normalize_identifier_token(m.group(1))
        left_col = _normalize_identifier_token(m.group(2))
        right_alias = _normalize_identifier_token(m.group(3))
        right_col = _normalize_identifier_token(m.group(4))
        left_table = alias_map.get(left_alias)
        right_table = alias_map.get(right_alias)
        if left_table and right_table and left_table in tables and right_table in tables:
            relaxed.add((left_alias, left_col))
            relaxed.add((right_alias, right_col))
    return relaxed


def _find_unknown_columns(
    sql: str,
    alias_map: dict[str, str],
    catalog: dict[str, Any],
) -> list[UnknownAliasColumnViolation]:
    tables = catalog.get("tables")
    if not isinstance(tables, dict) or not alias_map:
        return []

    join_on_refs = _extract_join_on_refs(sql, alias_map, tables)

    violations: list[UnknownAliasColumnViolation] = []
    seen: set[tuple[str, str, str]] = set()
    for match in _QUALIFIED_REF_PATTERN.finditer(sql):
        if match.start() > 0 and sql[match.start() - 1] == ":":
            continue
        token_1 = str(match.group(1) or "").strip()
        token_2 = str(match.group(2) or "").strip()
        token_3 = str(match.group(3) or "").strip()
        if token_3:
            alias_raw = token_2
            column_raw = token_3
        else:
            alias_raw = token_1
            column_raw = token_2

        alias_key = _normalize_identifier_token(alias_raw)
        column_key = _normalize_identifier_token(column_raw)
        if not alias_key or not column_key:
            continue

        table_key = alias_map.get(alias_key)
        if not table_key:
            continue
        table_payload = tables.get(table_key)
        if not isinstance(table_payload, dict):
            continue

        known_columns = {
            _normalize_identifier_token(value)
            for value in _as_string_list(table_payload.get("columns"))
        }
        known_columns.discard("")
        if not known_columns:
            continue
        if column_key in known_columns:
            continue
        if (alias_key, column_key) in join_on_refs:
            continue

        signature = (alias_key, column_key, table_key)
        if signature in seen:
            continue
        seen.add(signature)
        expected_table = str(table_payload.get("display_name", table_key)).strip() or table_key
        violations.append(
            UnknownAliasColumnViolation(
                alias=alias_raw,
                column=column_raw,
                expected_table=expected_table,
            )
        )
    return violations


def _find_unresolved_table_refs(
    sql: str,
    alias_map: dict[str, str],
    catalog: dict[str, Any],
) -> list[UnresolvedTableReferenceViolation]:
    """Detect table.column refs where the table exists in metadata but is not joined."""
    tables = catalog.get("tables")
    bare_to_full = catalog.get("bare_to_full")
    if not isinstance(tables, dict) or not isinstance(bare_to_full, dict):
        return []

    # Collect all alias keys that ARE resolved (from FROM/JOIN clauses)
    resolved_aliases = set(alias_map.keys())

    violations: list[UnresolvedTableReferenceViolation] = []
    seen: set[tuple[str, str]] = set()
    for match in _QUALIFIED_REF_PATTERN.finditer(sql):
        if match.start() > 0 and sql[match.start() - 1] == ":":
            continue
        token_1 = str(match.group(1) or "").strip()
        token_2 = str(match.group(2) or "").strip()
        token_3 = str(match.group(3) or "").strip()
        if token_3:
            # 3-part ref (schema.table.column): alias is token_2
            alias_raw = token_2
            column_raw = token_3
        else:
            # 2-part ref (alias.column): alias is token_1
            alias_raw = token_1
            column_raw = token_2

        alias_key = _normalize_identifier_token(alias_raw)
        column_key = _normalize_identifier_token(column_raw)
        if not alias_key or not column_key:
            continue

        # Skip if this alias is already resolved in FROM/JOIN
        if alias_key in resolved_aliases:
            continue

        # Check if alias_key matches a known metadata table (bare name)
        candidate_full_keys = bare_to_full.get(alias_key)
        if not candidate_full_keys:
            continue

        # Pick the display name from the first matching full table
        known_table = alias_key
        for full_key in _as_string_list(candidate_full_keys):
            table_payload = tables.get(full_key)
            if isinstance(table_payload, dict):
                known_table = (
                    str(table_payload.get("display_name", full_key)).strip()
                    or full_key
                )
                break

        signature = (alias_key, column_key)
        if signature in seen:
            continue
        seen.add(signature)
        violations.append(
            UnresolvedTableReferenceViolation(
                alias=alias_raw,
                column=column_raw,
                known_table=known_table,
            )
        )
    return violations


def _extract_column_names(
    raw_columns: Any,
    *,
    table_key: str,
    raw_joins: Any,
    raw_relationships: Any,
) -> set[str]:
    column_names: set[str] = set()
    if not isinstance(raw_columns, list):
        raw_columns = []
    for column in raw_columns:
        if isinstance(column, dict):
            raw_name = column.get("name", "")
        else:
            raw_name = column
        name = _normalize_identifier_token(str(raw_name))
        if name:
            column_names.add(name)
    column_names.update(_extract_join_column_names(table_key, raw_joins))
    column_names.update(_extract_join_column_names(table_key, raw_relationships))
    return column_names


def _extract_join_column_names(table_key: str, raw_joins: Any) -> set[str]:
    if not table_key or not isinstance(raw_joins, list):
        return set()
    out: set[str] = set()
    for join in raw_joins:
        if isinstance(join, dict):
            left_table = _normalize_qualified_identifier(
                str(join.get("left_table", ""))
            )
            right_table = _normalize_qualified_identifier(
                str(join.get("right_table", ""))
            )
            left_column = _normalize_identifier_token(str(join.get("left_column", "")))
            right_column = _normalize_identifier_token(
                str(join.get("right_column", ""))
            )
            if left_table == table_key and left_column:
                out.add(left_column)
            if right_table == table_key and right_column:
                out.add(right_column)
            continue
        if isinstance(join, str):
            out.update(_extract_join_column_names_from_text(table_key, join))
    return out


def _extract_join_column_names_from_text(table_key: str, join_text: str) -> set[str]:
    if not join_text:
        return set()
    out: set[str] = set()
    parts = str(join_text).split("=")
    if len(parts) != 2:
        return out
    left = _split_table_column_reference(parts[0])
    right = _split_table_column_reference(parts[1])
    if left is not None and left[0] == table_key:
        out.add(left[1])
    if right is not None and right[0] == table_key:
        out.add(right[1])
    return out


def _split_table_column_reference(value: str) -> tuple[str, str] | None:
    parts = _split_identifier_parts(value)
    if len(parts) < 2:
        return None
    column = parts[-1]
    table_parts = parts[:-1]
    if len(table_parts) >= 2:
        table_key = ".".join(table_parts[-2:])
    else:
        table_key = table_parts[0]
    if not table_key or not column:
        return None
    return table_key, column


def _table_name_from_document(doc: dict[str, Any]) -> str:
    schema = str(doc.get("schema", "")).strip()
    name = str(doc.get("name", "")).strip()
    if schema and name:
        return f"{schema}.{name}"
    if name:
        return name
    return str(doc.get("id", "")).strip()


def _split_identifier_parts(value: str) -> list[str]:
    raw_parts = re.findall(_IDENTIFIER_PATTERN, str(value))
    parts = [_normalize_identifier_token(part) for part in raw_parts]
    return [part for part in parts if part]


def _normalize_qualified_identifier(value: str) -> str:
    parts = _split_identifier_parts(value)
    if not parts:
        return ""
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return parts[0]


def _normalize_identifier_token(value: str) -> str:
    text = str(value).strip()
    if text.startswith('"') and text.endswith('"') and len(text) >= 2:
        text = text[1:-1].replace('""', '"')
    return text.lower()


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
