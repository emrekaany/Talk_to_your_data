"""Table-level metadata loaders and merge helpers."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any


class TableMetadataFileError(RuntimeError):
    """Raised when table metadata file is missing or malformed."""


def load_table_metadata_documents(table_metadata_path: Path) -> list[dict[str, Any]]:
    """Load table-level metadata documents."""
    if not table_metadata_path.exists():
        raise TableMetadataFileError(
            f"Missing table metadata file at '{table_metadata_path}'."
        )

    try:
        payload = json.loads(table_metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TableMetadataFileError(
            f"Table metadata file is not valid JSON: '{table_metadata_path}'."
        ) from exc

    if not isinstance(payload, dict):
        raise TableMetadataFileError(
            f"Table metadata root must be an object: '{table_metadata_path}'."
        )

    raw_documents = payload.get("documents")
    if not isinstance(raw_documents, list):
        raise TableMetadataFileError(
            f"Table metadata must contain a 'documents' list: '{table_metadata_path}'."
        )

    documents: list[dict[str, Any]] = []
    for index, raw_document in enumerate(raw_documents, start=1):
        if not isinstance(raw_document, dict):
            raise TableMetadataFileError(
                f"Table metadata document #{index} must be an object."
            )
        normalized = _normalize_table_metadata_document(raw_document, index=index)
        documents.append(normalized)
    return documents


def build_table_metadata_index(
    table_metadata_documents: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Build normalized table-key to metadata mapping."""
    index: dict[str, dict[str, Any]] = {}
    for document in table_metadata_documents:
        if not isinstance(document, dict):
            continue
        table_key = _document_table_key(document)
        if not table_key:
            continue
        table_metadata = document.get("table_metadata")
        if not isinstance(table_metadata, dict):
            continue
        index[table_key] = deepcopy(table_metadata)
    return index


def merge_table_metadata_into_documents(
    metadata_documents: list[dict[str, Any]],
    table_metadata_documents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach table metadata to base metadata documents and backfill removed keys."""
    table_index = build_table_metadata_index(table_metadata_documents)
    merged_documents: list[dict[str, Any]] = []
    for document in metadata_documents:
        if not isinstance(document, dict):
            continue
        merged_document = deepcopy(document)
        table_key = _document_table_key(merged_document)
        if not table_key:
            merged_documents.append(merged_document)
            continue
        table_metadata = table_index.get(table_key)
        if not isinstance(table_metadata, dict):
            merged_documents.append(merged_document)
            continue
        merged_document["table_metadata"] = deepcopy(table_metadata)
        for key, value in table_metadata.items():
            if key not in merged_document or _is_blank(merged_document.get(key)):
                merged_document[key] = deepcopy(value)
        merged_documents.append(merged_document)
    return merged_documents


def _normalize_table_metadata_document(
    raw_document: dict[str, Any],
    *,
    index: int,
) -> dict[str, Any]:
    schema = str(raw_document.get("schema", "")).strip()
    name = str(raw_document.get("name", "")).strip()
    identifier = str(raw_document.get("id", "")).strip()
    if not identifier:
        if schema and name:
            identifier = f"{schema}.{name}"
        elif name:
            identifier = name
    if not identifier:
        raise TableMetadataFileError(
            f"Table metadata document #{index} must include 'id' or 'schema'+'name'."
        )

    table_metadata = raw_document.get("table_metadata")
    if not isinstance(table_metadata, dict):
        raise TableMetadataFileError(
            f"Table metadata document #{index} must include object field 'table_metadata'."
        )
    return {
        "id": identifier,
        "schema": schema,
        "name": name,
        "table_metadata": deepcopy(table_metadata),
    }


def _document_table_key(document: dict[str, Any]) -> str:
    schema = str(document.get("schema", "")).strip()
    name = str(document.get("name", "")).strip()
    if schema and name:
        return f"{schema}.{name}".lower()
    if name:
        return name.lower()
    identifier = str(document.get("id", "")).strip()
    return identifier.lower()


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False
