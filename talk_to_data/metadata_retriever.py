"""Metadata loading and lightweight vector-style retrieval."""

from __future__ import annotations

from collections import Counter
import json
from math import sqrt
from pathlib import Path
import re
from typing import Any
import unicodedata


class MetadataFileError(RuntimeError):
    """Raised when a metadata file is missing, invalid, or unusable."""


def expected_metadata_schema_stub() -> dict[str, Any]:
    """Expected schema example for metadata files."""
    return {
        "documents": [
            {
                "doc_type": "table",
                "id": "AS_IFRS.IFRS_PRIM_VERISI",
                "schema": "AS_IFRS",
                "name": "IFRS_PRIM_VERISI",
                "description": "Business description",
                "grain": "1 row per key set",
                "mandatory_filters": ["STATUS = :status"],
                "performance_rules": ["Status filter is required"],
                "columns": [
                    {"name": "REPORT_PERIOD", "type": "NUMBER(6)"},
                    {"name": "PRIM_TL", "type": "NUMBER"},
                ],
                "joins": [
                    {
                        "left_table": "AS_IFRS.IFRS_PRIM_VERISI",
                        "left_column": "MUSTERI_NO",
                        "right_table": "AS_CUSTOMER.CUSTOMER_DIM",
                        "right_column": "MUSTERI_NO",
                    }
                ],
                "security": {
                    "restricted": True,
                    "pii_columns": [],
                    "note": "Do not run broad scans",
                },
            }
        ]
    }


def load_metadata_documents(metadata_path: Path) -> list[dict[str, Any]]:
    """Load vectored metadata in flexible schema forms."""
    if not metadata_path.exists():
        stub_path = metadata_path.with_name("metadata_vectored.schema.stub.json")
        if not stub_path.exists():
            stub_path.write_text(
                json.dumps(expected_metadata_schema_stub(), indent=2),
                encoding="utf-8",
            )
        raise MetadataFileError(
            f"Missing metadata file at '{metadata_path}'. "
            f"Created schema stub at '{stub_path}'."
        )

    try:
        raw = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MetadataFileError(
            f"metadata file is not valid JSON: {metadata_path}"
        ) from exc

    documents = _normalize_documents(_coerce_to_documents(raw))
    if not documents:
        stub_path = metadata_path.with_name("metadata_vectored.schema.stub.json")
        raise MetadataFileError(
            f"Metadata file '{metadata_path}' has no usable documents. "
            f"See '{stub_path}' for expected schema."
        )
    return documents


def build_metadata_overview(
    documents: list[dict[str, Any]],
    *,
    metadata_path: Path | None = None,
    general_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Small overview used for requirement extraction prompts."""
    tables: list[str] = []
    mandatory_filters: list[str] = []
    performance_rules: list[str] = []
    for doc in documents:
        table_name = _table_name(doc)
        if table_name:
            tables.append(table_name)
        for flt in _normalized_filter_list(doc.get("mandatory_filters")):
            if flt not in mandatory_filters:
                mandatory_filters.append(flt)

        for rule in _as_string_list(doc.get("performance_rules")):
            if rule and rule not in performance_rules:
                performance_rules.append(rule)

    metadata_source = str(metadata_path) if metadata_path is not None else ""
    overview = {
        "tables": tables[:30],
        "mandatory_filters": mandatory_filters[:20],
    }
    if metadata_source:
        overview["metadata_source"] = metadata_source
    if performance_rules:
        overview["performance_rules"] = performance_rules[:12]

    if general_metadata and isinstance(general_metadata, dict):
        raw_measures = general_metadata.get("measure_columns")
        if isinstance(raw_measures, list) and raw_measures:
            overview["measure_columns"] = raw_measures

    return overview


def retrieve_relevant_metadata(
    requirements: dict[str, Any],
    user_request: str,
    documents: list[dict[str, Any]] | None = None,
    *,
    metadata_path: Path | None = None,
    table_metadata_path: Path | None = None,
    top_k: int = 500,
) -> dict[str, Any]:
    """
    Retrieve token-efficient relevant metadata.

    Contract target: retrieve_relevant_metadata(requirements: dict, user_request: str) -> dict
    """
    metadata_source = str(metadata_path) if metadata_path is not None else "in_memory_documents"
    table_metadata_source = (
        str(table_metadata_path) if table_metadata_path is not None else ""
    )
    if documents is None:
        if metadata_path is None:
            metadata_path = Path("metadata_vectored.json")
        metadata_source = str(metadata_path)
        documents = load_metadata_documents(metadata_path)
    else:
        documents = _normalize_documents(documents)

    effective_top_k = max(1, min(_safe_int(top_k, fallback=500), 500))
    min_score_threshold = 0.005

    query_tokens = _query_tokens(requirements, user_request)
    scored: list[tuple[float, dict[str, Any]]] = []
    for doc in documents:
        score = _cosine_similarity(query_tokens, _tokenize(_doc_to_search_text(doc)))
        scored.append((score, doc))
    scored.sort(key=lambda pair: pair[0], reverse=True)

    high_confidence = [item for item in scored if item[0] >= min_score_threshold]
    if len(scored) <= effective_top_k:
        selected = scored
    elif len(high_confidence) >= effective_top_k:
        selected = high_confidence[:effective_top_k]
    else:
        selected = scored[:effective_top_k]

    relevant_items = [
        _compact_doc(doc, query_tokens=query_tokens, score=score)
        for score, doc in selected
    ]
    guardrails = _collect_guardrails(relevant_items)
    mandatory_rules = _collect_mandatory_rules(requirements, relevant_items)

    return {
        "dialect": "oracle sql",
        "relevant_items": relevant_items,
        "guardrails": guardrails,
        "mandatory_rules": mandatory_rules,
        "runtime_mandatory_rules": [],
        "retrieval_debug": {
            "selected_count": len(relevant_items),
            "total_documents": len(documents),
            "effective_top_k": effective_top_k,
            "min_score_threshold": min_score_threshold,
            "metadata_source": metadata_source,
            "table_metadata_source": table_metadata_source,
        },
    }


def _coerce_to_documents(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [doc for doc in raw if isinstance(doc, dict)]
    if isinstance(raw, dict):
        for key in ("documents", "chunks", "items", "data"):
            value = raw.get(key)
            if isinstance(value, list):
                return [doc for doc in value if isinstance(doc, dict)]
        if _looks_like_document(raw):
            return [raw]
    return []


def _normalize_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for doc in documents:
        normalized_doc = dict(doc)
        if "Keywords" in normalized_doc and "keywords" not in normalized_doc:
            normalized_doc["keywords"] = normalized_doc.pop("Keywords")
        raw_columns = normalized_doc.get("columns")
        if isinstance(raw_columns, list):
            normalized_columns: list[dict[str, Any]] = []
            for raw_column in raw_columns:
                if isinstance(raw_column, dict):
                    normalized_columns.append(_normalize_column(raw_column))
            normalized_doc["columns"] = normalized_columns
        normalized.append(normalized_doc)
    return normalized


def _validate_join_key_columns(
    documents: list[dict[str, Any]],
    *,
    metadata_path: Path,
) -> None:
    table_to_columns: dict[str, set[str]] = {}
    bare_to_tables: dict[str, set[str]] = {}

    for doc in documents:
        table_name = _table_name(doc)
        table_key = _normalize_table_name(table_name)
        if not table_key:
            continue
        columns: set[str] = set()
        raw_columns = doc.get("columns")
        if isinstance(raw_columns, list):
            for col in raw_columns:
                if not isinstance(col, dict):
                    continue
                column_name = _normalize_column_name(col.get("name"))
                if column_name:
                    columns.add(column_name)
        table_to_columns[table_key] = columns
        bare_to_tables.setdefault(_table_bare_name(table_key), set()).add(table_key)

    violations: list[str] = []
    for doc in documents:
        source_table = _normalize_table_name(_table_name(doc))
        raw_joins = doc.get("joins")
        if not isinstance(raw_joins, list):
            continue
        for index, join in enumerate(raw_joins, start=1):
            if not isinstance(join, dict):
                continue
            left_table = _normalize_table_name(str(join.get("left_table", "")).strip())
            right_table = _normalize_table_name(str(join.get("right_table", "")).strip())
            left_column = _normalize_column_name(join.get("left_column"))
            right_column = _normalize_column_name(join.get("right_column"))
            if not left_table or not right_table or not left_column or not right_column:
                continue

            left_resolved, left_error = _resolve_join_table(left_table, table_to_columns, bare_to_tables)
            right_resolved, right_error = _resolve_join_table(right_table, table_to_columns, bare_to_tables)
            if left_error:
                violations.append(
                    f"{source_table or '<unknown>'} join[{index}] {left_error}"
                )
                continue
            if right_error:
                violations.append(
                    f"{source_table or '<unknown>'} join[{index}] {right_error}"
                )
                continue
            if left_resolved is None or right_resolved is None:
                continue

            left_columns = table_to_columns.get(left_resolved, set())
            right_columns = table_to_columns.get(right_resolved, set())
            if left_column not in left_columns:
                violations.append(
                    f"{source_table or '<unknown>'} join[{index}] unknown left column "
                    f"'{left_table}.{left_column}' (resolved table: {left_resolved})."
                )
            if right_column not in right_columns:
                violations.append(
                    f"{source_table or '<unknown>'} join[{index}] unknown right column "
                    f"'{right_table}.{right_column}' (resolved table: {right_resolved})."
                )

    if violations:
        sample = "; ".join(violations[:8])
        if len(violations) > 8:
            sample += f"; ... (+{len(violations) - 8} more)"
        raise MetadataFileError(
            "Metadata join-key validation failed. "
            f"Path: '{metadata_path}'. Details: {sample}"
        )


def _resolve_join_table(
    table_name: str,
    table_to_columns: dict[str, set[str]],
    bare_to_tables: dict[str, set[str]],
) -> tuple[str | None, str | None]:
    if not table_name:
        return None, "join table name is empty."
    if table_name in table_to_columns:
        return table_name, None

    bare = _table_bare_name(table_name)
    candidates = sorted(bare_to_tables.get(bare, set()))
    if not candidates:
        return None, f"join table '{table_name}' does not exist in metadata documents."
    if len(candidates) > 1:
        return None, (
            f"join table '{table_name}' is ambiguous across schemas: {', '.join(candidates)}."
        )
    return candidates[0], None


def _normalize_column(raw_column: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw_column)
    if "Keywords" in normalized and "keywords" not in normalized:
        normalized["keywords"] = normalized.pop("Keywords")
    if "Type" in normalized and "semantic_type" not in normalized:
        normalized["semantic_type"] = normalized.pop("Type")
    return normalized


def _looks_like_document(doc: dict[str, Any]) -> bool:
    return any(key in doc for key in ("name", "schema", "columns", "doc_type", "id"))


def _table_name(doc: dict[str, Any]) -> str:
    schema = str(doc.get("schema", "")).strip()
    name = str(doc.get("name", "")).strip()
    if schema and name:
        return f"{schema}.{name}"
    if name:
        return name
    identifier = str(doc.get("id", "")).strip()
    return identifier


def _normalize_table_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace('"', "")
    parts = [part.strip().lower() for part in text.split(".") if part.strip()]
    if not parts:
        return ""
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return parts[0]


def _table_bare_name(table_name: str) -> str:
    normalized = _normalize_table_name(table_name)
    if not normalized:
        return ""
    return normalized.split(".")[-1]


def _normalize_column_name(value: Any) -> str:
    text = str(value or "").strip().replace('"', "")
    return text.upper() if text else ""


def _query_tokens(
    requirements: dict[str, Any],
    user_request: str,
    general_metadata: dict[str, Any] | None = None,
) -> Counter[str]:
    texts: list[str] = [user_request]
    for key in ("intent", "notes"):
        value = requirements.get(key)
        if value:
            texts.append(str(value))
    for key in ("required_filters", "measures", "dimensions", "grain", "join_needs"):
        for item in _as_string_list(requirements.get(key)):
            texts.append(item)
    base_tokens = _tokenize(" ".join(texts))

    # Expand with domain vocabulary from general_metadata
    if general_metadata and isinstance(general_metadata, dict):
        vocab = general_metadata.get("domain_vocabulary")
        if isinstance(vocab, dict):
            expansion_tokens: Counter[str] = Counter()
            for term, identifiers in vocab.items():
                normalized_term = unicodedata.normalize("NFKC", term).lower()
                if normalized_term in base_tokens:
                    for ident in _as_string_list(identifiers):
                        for tok in _tokenize(ident):
                            expansion_tokens[tok] += 1
            # Add expansion tokens at half-weight to avoid over-boosting
            for tok, count in expansion_tokens.items():
                base_tokens[tok] += max(1, count // 2)

    return base_tokens


def _doc_to_search_text(doc: dict[str, Any]) -> str:
    parts: list[str] = [
        str(doc.get("doc_type", "")),
        str(doc.get("id", "")),
        str(doc.get("schema", "")),
        str(doc.get("name", "")),
        str(doc.get("description", "")),
        str(doc.get("grain", "")),
    ]
    for key in ("keywords", "mandatory_filters", "performance_rules", "business_notes"):
        for value in _as_string_list(doc.get(key)):
            parts.append(value)
    for value in _as_string_list(doc.get("indexes")):
        parts.append(value)
    for column in doc.get("columns", []):
        if not isinstance(column, dict):
            continue
        parts.append(str(column.get("name", "")))
        parts.append(str(column.get("description", "")))
        parts.append(str(column.get("type", "")))
        parts.append(str(column.get("semantic_type", "")))
        for keyword in _as_string_list(column.get("keywords")):
            parts.append(keyword)
    security = doc.get("security")
    if isinstance(security, dict):
        parts.append(str(security.get("note", "")))
        for pii_col in _as_string_list(security.get("pii_columns")):
            parts.append(pii_col)
    return " ".join(parts)


def _compact_doc(
    doc: dict[str, Any],
    *,
    query_tokens: Counter[str],
    score: float,
) -> dict[str, Any]:
    table = _table_name(doc)
    columns = _select_columns(doc, query_tokens)
    joins = _extract_joins(doc)
    mandatory_filters = _normalized_filter_list(doc.get("mandatory_filters"))

    return {
        "score": round(score, 4),
        "table": table,
        "description": str(doc.get("description", "")).strip(),
        "grain": str(doc.get("grain", "")).strip(),
        "columns": columns,
        "joins": joins,
        "indexes": _extract_indexes(doc),
        "mandatory_filters": mandatory_filters,
        "performance_rules": _as_string_list(doc.get("performance_rules")),
        "security": doc.get("security", {}),
        "keywords": _as_string_list(doc.get("keywords"))[:20],
        "table_metadata": _extract_table_metadata(doc),
    }


def _extract_table_metadata(doc: dict[str, Any]) -> dict[str, Any]:
    raw_table_metadata = doc.get("table_metadata")
    if isinstance(raw_table_metadata, dict):
        return raw_table_metadata

    table_keys = (
        "description",
        "grain",
        "keywords",
        "business_notes",
        "table_types",
        "parent_business_assets",
        "primary_key_candidate",
        "mandatory_filters",
        "performance_rules",
        "relationships",
        "security",
        "source_lineage",
        "business_assets",
        "version",
        "updated_at",
        "agent_lookup_sheet_stats",
        "agent_lookup_sample_rows",
    )
    metadata: dict[str, Any] = {}
    for key in table_keys:
        value = doc.get(key)
        if value in (None, "", [], {}):
            continue
        metadata[key] = value
    return metadata


def _select_columns(doc: dict[str, Any], query_tokens: Counter[str]) -> list[dict[str, Any]]:
    raw_columns = doc.get("columns")
    if not isinstance(raw_columns, list):
        return []

    scored_columns: list[tuple[float, dict[str, Any]]] = []
    for raw_column in raw_columns:
        if not isinstance(raw_column, dict):
            continue
        name = str(raw_column.get("name", "")).strip()
        if not name:
            continue
        column_text = " ".join(
            [
                name,
                str(raw_column.get("description", "")),
                str(raw_column.get("type", "")),
                str(raw_column.get("semantic_type", "")),
                " ".join(_as_string_list(raw_column.get("keywords"))),
                " ".join(_column_properties(raw_column)),
            ]
        )
        score = _cosine_similarity(query_tokens, _tokenize(column_text))
        scored_columns.append(
            (
                score,
                _compact_column(raw_column),
            )
        )

    scored_columns.sort(key=lambda pair: pair[0], reverse=True)
    selected = [column for score, column in scored_columns if score > 0]
    if not selected:
        selected = [column for _, column in scored_columns]
    if len(selected) < 40:
        selected = [column for _, column in scored_columns]
    return selected


def _compact_column(raw_column: dict[str, Any]) -> dict[str, Any]:
    column: dict[str, Any] = {
        "name": str(raw_column.get("name", "")).strip(),
        "type": str(raw_column.get("type", "")).strip(),
        "description": str(raw_column.get("description", "")).strip(),
    }
    semantic_type = str(raw_column.get("semantic_type", "")).strip()
    if semantic_type:
        column["semantic_type"] = semantic_type

    keywords = _as_string_list(raw_column.get("keywords"))
    if keywords:
        column["keywords"] = keywords[:15]

    properties = _column_properties(raw_column)
    if properties:
        column["properties"] = properties

    select_expressions = _as_string_list(raw_column.get("select_expressions"))
    if select_expressions:
        column["select_expressions"] = select_expressions[:5]
    return column


def _column_properties(raw_column: dict[str, Any]) -> list[str]:
    property_keys = (
        "nullable",
        "is_nullable",
        "format",
        "precision",
        "scale",
        "unit",
        "domain",
        "default",
        "example",
    )
    properties: list[str] = []
    for key in property_keys:
        value = raw_column.get(key)
        if value in (None, "", []):
            continue
        properties.append(f"{key}={_short_property_value(value)}")
    return properties[:8]


def _short_property_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        text = ",".join(str(item).strip() for item in value if str(item).strip())
    else:
        text = str(value).strip()
    text = _normalize_space(text)
    if len(text) > 80:
        return f"{text[:77]}..."
    return text


def _extract_joins(doc: dict[str, Any]) -> list[str]:
    joins: list[str] = []
    raw_joins = doc.get("joins")
    if isinstance(raw_joins, list):
        for join in raw_joins:
            if isinstance(join, str):
                joins.append(join)
                continue
            if isinstance(join, dict):
                left_table = str(join.get("left_table", "")).strip()
                left_column = str(join.get("left_column", "")).strip()
                right_table = str(join.get("right_table", "")).strip()
                right_column = str(join.get("right_column", "")).strip()
                if left_table and left_column and right_table and right_column:
                    joins.append(
                        f"{left_table}.{left_column} = {right_table}.{right_column}"
                    )
    relationships = doc.get("relationships")
    if isinstance(relationships, list):
        for rel in relationships:
            if isinstance(rel, str):
                joins.append(rel)
    seen: set[str] = set()
    deduped: list[str] = []
    for j in joins:
        key = j.strip().lower()
        if key not in seen:
            seen.add(key)
            deduped.append(j)
    return deduped[:20]


def _extract_indexes(doc: dict[str, Any]) -> list[str]:
    raw_indexes = doc.get("indexes")
    if raw_indexes is None:
        return []
    if isinstance(raw_indexes, str):
        return [raw_indexes]
    if isinstance(raw_indexes, list):
        output: list[str] = []
        for item in raw_indexes:
            if isinstance(item, dict):
                table = str(item.get("table", "")).strip()
                columns = item.get("columns")
                if table and isinstance(columns, list) and columns:
                    output.append(f"{table}({', '.join(str(c) for c in columns)})")
                    continue
            text = str(item).strip()
            if text:
                output.append(text)
        return output[:20]
    return [str(raw_indexes).strip()]


def _collect_guardrails(relevant_items: list[dict[str, Any]]) -> list[str]:
    rules: list[str] = []
    for item in relevant_items:
        for perf in _as_string_list(item.get("performance_rules")):
            if perf not in rules:
                rules.append(perf)
        security = item.get("security")
        if isinstance(security, dict):
            note = str(security.get("note", "")).strip()
            if note and note not in rules:
                rules.append(note)
            pii_columns = _as_string_list(security.get("pii_columns"))
            if pii_columns:
                pii_rule = f"PII columns restricted: {', '.join(pii_columns)}"
                if pii_rule not in rules:
                    rules.append(pii_rule)
    return rules[:25]


def _collect_mandatory_rules(
    requirements: dict[str, Any],
    relevant_items: list[dict[str, Any]],
) -> list[str]:
    mandatory: list[str] = []
    for flt in _normalized_filter_list(requirements.get("required_filters")):
        if flt not in mandatory:
            mandatory.append(flt)
    for item in relevant_items:
        for flt in _normalized_filter_list(item.get("mandatory_filters")):
            if flt not in mandatory:
                mandatory.append(flt)
    return mandatory


def _tokenize(text: str) -> Counter[str]:
    normalized = unicodedata.normalize("NFKC", text).lower()
    tokens = re.findall(r"\w{2,}", normalized, flags=re.UNICODE)
    return Counter(tokens)


def _cosine_similarity(a: Counter[str], b: Counter[str]) -> float:
    if not a or not b:
        return 0.0
    overlap = set(a.keys()) & set(b.keys())
    numerator = sum(a[token] * b[token] for token in overlap)
    denom_a = sqrt(sum(value * value for value in a.values()))
    denom_b = sqrt(sum(value * value for value in b.values()))
    if denom_a == 0 or denom_b == 0:
        return 0.0
    return float(numerator / (denom_a * denom_b))


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                result.append(text)
        return result
    return [str(value).strip()]


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _normalized_filter_list(value: Any) -> list[str]:
    normalized: list[str] = []
    for text in _as_string_list(value):
        expr = _normalize_filter_expression(text)
        if expr and expr not in normalized:
            normalized.append(expr)
    return normalized


def _normalize_filter_expression(filter_text: str) -> str:
    text = _normalize_space(filter_text)
    if not text:
        return ""
    return text


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Column-based retrieval (v2)
# ---------------------------------------------------------------------------


def load_column_metadata(column_metadata_path: Path) -> dict[str, Any]:
    """Load column_metadata_{agent}.json and return the parsed payload."""
    if not column_metadata_path.exists():
        raise MetadataFileError(
            f"Missing column metadata file at '{column_metadata_path}'."
        )
    try:
        payload = json.loads(column_metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise MetadataFileError(
            f"Column metadata file is not valid JSON: '{column_metadata_path}'."
        ) from exc
    if not isinstance(payload, dict):
        raise MetadataFileError(
            f"Column metadata root must be an object: '{column_metadata_path}'."
        )
    columns = payload.get("columns")
    if not isinstance(columns, list) or not columns:
        raise MetadataFileError(
            f"Column metadata must contain a non-empty 'columns' list: '{column_metadata_path}'."
        )
    return payload


def retrieve_column_based_metadata(
    requirements: dict[str, Any],
    user_request: str,
    column_metadata: dict[str, Any],
    table_metadata_index: dict[str, dict[str, Any]],
    *,
    top_k: int = 15,
    column_metadata_path: Path | None = None,
    table_metadata_path: Path | None = None,
    general_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Column-level retrieval: score individual columns, group by table, enrich with table metadata.

    Returns a prompt-ready metadata dict compatible with _metadata_prompt_text().
    """
    columns = column_metadata.get("columns", [])
    global_notes = _as_string_list(
        (general_metadata or {}).get("global_reporting_notes")
        or column_metadata.get("global_reporting_notes")
    )
    effective_top_k = max(1, min(_safe_int(top_k, fallback=15), 60))
    min_score = 0.01

    query_tokens = _query_tokens(requirements, user_request, general_metadata)

    # Score each column
    scored: list[tuple[float, dict[str, Any]]] = []
    for col in columns:
        if not isinstance(col, dict):
            continue
        search_text = " ".join([
            str(col.get("table", "")),
            str(col.get("name", "")),
            str(col.get("description", "")),
            str(col.get("semantic_type", "")),
            " ".join(_as_string_list(col.get("keywords"))),
        ])
        score = _cosine_similarity(query_tokens, _tokenize(search_text))
        scored.append((score, col))

    scored.sort(key=lambda pair: pair[0], reverse=True)

    # Select top-k columns above threshold
    selected = [(s, c) for s, c in scored if s >= min_score][:effective_top_k]
    if not selected and scored:
        selected = scored[:effective_top_k]

    # Group selected columns by table
    table_columns: dict[str, list[dict[str, Any]]] = {}
    table_max_score: dict[str, float] = {}
    for score, col in selected:
        table_id = str(col.get("table", "")).strip()
        if not table_id:
            continue
        table_columns.setdefault(table_id, []).append(col)
        table_max_score[table_id] = max(table_max_score.get(table_id, 0.0), score)

    # Backfill: include ALL columns from each matched table for complete allowlist
    all_columns_by_table_lower: dict[str, list[dict[str, Any]]] = {}
    for col in columns:
        if isinstance(col, dict):
            t = str(col.get("table", "")).strip()
            if t:
                all_columns_by_table_lower.setdefault(t.lower(), []).append(col)

    for table_id in list(table_columns.keys()):
        existing_names = {
            str(c.get("name", "")).strip().upper()
            for c in table_columns[table_id]
            if isinstance(c, dict)
        }
        all_cols = all_columns_by_table_lower.get(table_id.lower(), [])
        for col in all_cols:
            col_name = str(col.get("name", "")).strip().upper()
            if col_name and col_name not in existing_names:
                table_columns[table_id].append(col)
                existing_names.add(col_name)

    # Build matched table set for join filtering
    matched_tables = set(table_columns.keys())
    matched_tables_lower = {t.lower() for t in matched_tables}

    # Auto-include bridge tables that connect two or more matched tables
    bridge_tables_lower = _find_bridge_tables(matched_tables_lower, table_metadata_index)
    if bridge_tables_lower:
        known_tables = _as_string_list(
            (general_metadata or {}).get("known_tables")
            or column_metadata.get("known_tables")
        )
        lower_to_original = {
            t.strip().lower(): t.strip() for t in known_tables if t.strip()
        }
        all_columns_by_table: dict[str, list[dict[str, Any]]] = {}
        for col in columns:
            if isinstance(col, dict):
                t = str(col.get("table", "")).strip()
                if t:
                    all_columns_by_table.setdefault(t.lower(), []).append(col)
        for bridge_lower in bridge_tables_lower:
            bridge_original = lower_to_original.get(bridge_lower, bridge_lower)
            if bridge_original not in table_columns:
                table_columns[bridge_original] = all_columns_by_table.get(
                    bridge_lower, []
                )
                table_max_score[bridge_original] = 0.0
                matched_tables.add(bridge_original)
                matched_tables_lower.add(bridge_lower)

    # Inject core tables for aggregation queries if missing
    if general_metadata and isinstance(general_metadata, dict):
        core_tables = _as_string_list(general_metadata.get("core_tables"))
        if core_tables:
            _inject_core_tables_if_needed(
                requirements, core_tables, columns,
                table_columns, table_max_score,
                matched_tables, matched_tables_lower,
            )

    # Build relevant_items (compatible with existing prompt renderer)
    relevant_items: list[dict[str, Any]] = []
    for table_id in sorted(table_columns, key=lambda t: table_max_score.get(t, 0), reverse=True):
        cols = table_columns[table_id]
        tm_key = table_id.lower()
        tm = table_metadata_index.get(tm_key, {})

        # Filter joins to only include matched tables
        all_joins = _as_string_list(tm.get("relationships"))
        filtered_joins = _filter_joins_to_matched(all_joins, matched_tables_lower)

        item: dict[str, Any] = {
            "score": round(table_max_score.get(table_id, 0.0), 4),
            "table": table_id,
            "columns": cols,
            "joins": filtered_joins,
            "indexes": _as_string_list(tm.get("indexes")),
            "table_metadata": {
                k: v for k, v in tm.items()
                if k in ("description", "grain", "keywords", "business_notes",
                         "performance_rules", "mandatory_filters",
                         "join_definitions")
                and v not in (None, "", [], {})
            },
            "mandatory_filters": _as_string_list(tm.get("mandatory_filters")),
            "performance_rules": _as_string_list(tm.get("performance_rules")),
        }
        relevant_items.append(item)

    guardrails = _collect_guardrails(relevant_items)
    mandatory_rules = _collect_mandatory_rules(requirements, relevant_items)

    return {
        "dialect": "oracle sql",
        "relevant_items": relevant_items,
        "guardrails": guardrails,
        "mandatory_rules": mandatory_rules,
        "runtime_mandatory_rules": [],
        "global_reporting_notes": global_notes,
        "core_tables": _as_string_list((general_metadata or {}).get("core_tables")),
        "retrieval_debug": {
            "retrieval_mode": "column_based",
            "selected_column_count": len(selected),
            "matched_table_count": len(table_columns),
            "total_columns": len(columns),
            "effective_top_k": effective_top_k,
            "min_score": min_score,
            "column_metadata_source": str(column_metadata_path or ""),
            "table_metadata_source": str(table_metadata_path or ""),
        },
    }


def _find_bridge_tables(
    matched_tables_lower: set[str],
    table_metadata_index: dict[str, dict[str, Any]],
) -> set[str]:
    """Find non-matched tables that bridge two or more matched tables (single hop)."""
    bridge_candidates: dict[str, set[str]] = {}
    for matched_table in matched_tables_lower:
        tm = table_metadata_index.get(matched_table, {})
        for rel in _as_string_list(tm.get("relationships")):
            parts = rel.strip().split("=")
            if len(parts) != 2:
                continue
            left_table = _extract_table_from_qualified(parts[0].strip()).lower()
            right_table = _extract_table_from_qualified(parts[1].strip()).lower()
            for ref_table in (left_table, right_table):
                if ref_table and ref_table not in matched_tables_lower:
                    bridge_candidates.setdefault(ref_table, set()).add(matched_table)
    return {t for t, connected in bridge_candidates.items() if len(connected) >= 2}


def _filter_joins_to_matched(
    joins: list[str],
    matched_tables_lower: set[str],
) -> list[str]:
    """Keep only join conditions where both sides reference a matched table."""
    filtered: list[str] = []
    seen: set[str] = set()
    for join_str in joins:
        key = join_str.strip().lower()
        if key in seen:
            continue
        # Parse "SCHEMA.TABLE.COL = SCHEMA.TABLE.COL" pattern
        parts = key.split("=")
        if len(parts) != 2:
            continue
        left = parts[0].strip()
        right = parts[1].strip()
        left_table = _extract_table_from_qualified(left)
        right_table = _extract_table_from_qualified(right)
        if left_table in matched_tables_lower and right_table in matched_tables_lower:
            seen.add(key)
            filtered.append(join_str)
    return filtered


def _extract_table_from_qualified(qualified_col: str) -> str:
    """Extract schema.table from schema.table.column or table.column."""
    parts = qualified_col.strip().split(".")
    if len(parts) >= 3:
        return f"{parts[-3]}.{parts[-2]}"
    if len(parts) == 2:
        return parts[0]
    return qualified_col.strip()


# ---------------------------------------------------------------------------
# Core-table safety net
# ---------------------------------------------------------------------------

_AGGREGATION_INTENTS = frozenset({
    "metric", "retrieve_data", "total_kpk", "comparison",
    "sum", "average", "count", "aggregation",
})


def _inject_core_tables_if_needed(
    requirements: dict[str, Any],
    core_tables: list[str],
    all_columns: list[dict[str, Any]],
    table_columns: dict[str, list[dict[str, Any]]],
    table_max_score: dict[str, float],
    matched_tables: set[str],
    matched_tables_lower: set[str],
) -> None:
    """Ensure at least one core (fact) table is present for aggregation intents."""
    intent = str(requirements.get("intent", "")).lower().strip()
    measures = _as_string_list(requirements.get("measures"))
    if intent not in _AGGREGATION_INTENTS and not measures:
        return

    # Check if any core table already matched
    core_lower = {t.lower() for t in core_tables}
    if core_lower & matched_tables_lower:
        return

    # No core table found — inject all core tables with minimal score
    all_columns_by_table: dict[str, list[dict[str, Any]]] = {}
    for col in all_columns:
        if isinstance(col, dict):
            t = str(col.get("table", "")).strip()
            if t:
                all_columns_by_table.setdefault(t.lower(), []).append(col)

    for core_table in core_tables:
        core_key = core_table.lower()
        if core_key not in matched_tables_lower:
            cols = all_columns_by_table.get(core_key, [])
            if cols:
                table_columns[core_table] = cols
                table_max_score[core_table] = 0.001
                matched_tables.add(core_table)
                matched_tables_lower.add(core_key)


def load_general_metadata(general_metadata_path: Path) -> dict[str, Any]:
    """Load general_metadata_{agent}.json and return the parsed payload."""
    if not general_metadata_path.exists():
        return {}
    try:
        payload = json.loads(general_metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload
