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
                "mandatory_filters": ["REPORT_PERIOD = :report_period"],
                "performance_rules": ["REPORT_PERIOD filter is required"],
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
    _validate_join_key_columns(documents, metadata_path=metadata_path)
    return documents


def build_metadata_overview(
    documents: list[dict[str, Any]],
    *,
    metadata_path: Path | None = None,
) -> dict[str, Any]:
    """Small overview used for requirement extraction prompts."""
    tables: list[str] = []
    mandatory_filters: list[str] = []
    performance_rules: list[str] = []
    has_report_period_column = False
    for doc in documents:
        table_name = _table_name(doc)
        if table_name:
            tables.append(table_name)
        for flt in _normalized_filter_list(doc.get("mandatory_filters")):
            if flt not in mandatory_filters:
                mandatory_filters.append(flt)
        partitioning = doc.get("partitioning")
        if isinstance(partitioning, dict) and partitioning.get("mandatory_filter"):
            column = str(partitioning.get("column", "REPORT_PERIOD"))
            expr = f"{column} = :report_period"
            if expr not in mandatory_filters:
                mandatory_filters.append(expr)

        if not has_report_period_column and _doc_has_column(doc, "REPORT_PERIOD"):
            has_report_period_column = True

        for rule in _as_string_list(doc.get("performance_rules")):
            if rule and rule not in performance_rules:
                performance_rules.append(rule)

    metadata_source = str(metadata_path) if metadata_path is not None else ""
    time_filter_policy = _detect_time_filter_policy(
        performance_rules=performance_rules,
        metadata_source=metadata_source,
    )

    overview = {
        "tables": tables[:30],
        "mandatory_filters": mandatory_filters[:20],
        "has_report_period_column": has_report_period_column,
    }
    if metadata_source:
        overview["metadata_source"] = metadata_source
    if performance_rules:
        overview["performance_rules"] = performance_rules[:12]
    if time_filter_policy:
        overview["time_filter_policy"] = time_filter_policy
    return overview


def retrieve_relevant_metadata(
    requirements: dict[str, Any],
    user_request: str,
    documents: list[dict[str, Any]] | None = None,
    *,
    metadata_path: Path | None = None,
    top_k: int = 200,
) -> dict[str, Any]:
    """
    Retrieve token-efficient relevant metadata.

    Contract target: retrieve_relevant_metadata(requirements: dict, user_request: str) -> dict
    """
    metadata_source = str(metadata_path) if metadata_path is not None else "in_memory_documents"
    if documents is None:
        if metadata_path is None:
            metadata_path = Path("metadata_vectored.json")
        metadata_source = str(metadata_path)
        documents = load_metadata_documents(metadata_path)
    else:
        documents = _normalize_documents(documents)

    effective_top_k = max(1, min(_safe_int(top_k, fallback=200), 200))
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


def _doc_has_column(doc: dict[str, Any], column_name: str) -> bool:
    raw_columns = doc.get("columns")
    if not isinstance(raw_columns, list):
        return False
    target = column_name.strip().upper()
    if not target:
        return False
    for column in raw_columns:
        if not isinstance(column, dict):
            continue
        name = str(column.get("name", "")).strip().upper()
        if name == target:
            return True
    return False


def _detect_time_filter_policy(
    *,
    performance_rules: list[str],
    metadata_source: str,
) -> str | None:
    source_low = metadata_source.lower()
    is_uretim_source = "uretim" in source_low
    if not is_uretim_source:
        return None

    text = _normalize_space(" ".join(performance_rules)).lower()
    if "ek tanzim" in text:
        return "ek_tanzim_date"
    return None


def _query_tokens(requirements: dict[str, Any], user_request: str) -> Counter[str]:
    texts: list[str] = [user_request]
    for key in ("intent", "report_period", "notes"):
        value = requirements.get(key)
        if value:
            texts.append(str(value))
    for key in ("required_filters", "measures", "dimensions", "grain", "join_needs"):
        for item in _as_string_list(requirements.get(key)):
            texts.append(item)
    return _tokenize(" ".join(texts))


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

    partitioning = doc.get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("mandatory_filter"):
        column = str(partitioning.get("column", "REPORT_PERIOD")).strip()
        if column:
            expr = _normalize_filter_expression(f"{column} = :report_period")
            if expr not in mandatory_filters:
                mandatory_filters.append(expr)

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
    }


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
    return selected[:60]


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
        column["keywords"] = keywords[:8]

    properties = _column_properties(raw_column)
    if properties:
        column["properties"] = properties
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
    return joins[:20]


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
    if re.search(r"\b(and|or)\b", text, flags=re.IGNORECASE):
        return text
    if re.search(r"(=|<>|!=|<=|>=|<|>|\blike\b|\bbetween\b|\bin\b|\bis\b)", text, flags=re.IGNORECASE):
        return text

    token_match = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text)
    if not token_match:
        return text

    column = token_match.group(0).upper()
    bind_name = "report_period" if column.lower() == "report_period" else column.lower()
    return f"{column} = :{bind_name}"


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
