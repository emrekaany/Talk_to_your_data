"""One-time script: split metadata_vectored into column_metadata + clean table_metadata.

Usage:
    python scripts/split_metadata.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

AGENTS = ("uretim", "hasar", "satis")
METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata" / "agents"

# Max description length after cleanup
MAX_DESC_LEN = 300

# Keys to KEEP in cleaned table_metadata
TABLE_METADATA_KEEP_KEYS = frozenset({
    "description",
    "grain",
    "keywords",
    "business_notes",
    "performance_rules",
    "relationships",
    "mandatory_filters",
})


def shorten_description(raw: str) -> str:
    """Remove inline lookup data from column descriptions.

    Pattern: '[description text] Kolon icerigi: [lookup rows] Tablo baglami: [context]'
    We keep the description text and optionally the table context.
    """
    text = raw.strip()
    if not text:
        return ""

    # Extract table context (appears after "Tablo baglami:")
    table_context = ""
    tablo_match = re.search(r"Tablo baglami:\s*(.+?)$", text, re.IGNORECASE)
    if tablo_match:
        table_context = tablo_match.group(1).strip()

    # Remove everything from "Kolon icerigi:" onwards
    kolon_idx = text.lower().find("kolon icerigi:")
    if kolon_idx > 0:
        text = text[:kolon_idx].strip()

    # Append table context if space allows
    if table_context and len(text) + len(table_context) + 3 <= MAX_DESC_LEN:
        text = f"{text} ({table_context})"

    # Also strip "Acente Bilgileri sheet column: XXX" patterns
    text = re.sub(r"\s*Acente Bilgileri sheet column:\s*\S+", "", text).strip()

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Truncate if still too long
    if len(text) > MAX_DESC_LEN:
        text = text[: MAX_DESC_LEN - 3] + "..."

    return text


def extract_columns(documents: list[dict]) -> list[dict]:
    """Extract flat column list from vectored documents."""
    columns = []
    for doc in documents:
        schema = str(doc.get("schema", "")).strip()
        name = str(doc.get("name", "")).strip()
        if schema and name:
            table_id = f"{schema}.{name}"
        elif name:
            table_id = name
        else:
            table_id = str(doc.get("id", "")).strip()
        if not table_id:
            continue

        raw_columns = doc.get("columns")
        if not isinstance(raw_columns, list):
            continue

        for col in raw_columns:
            if not isinstance(col, dict):
                continue
            col_name = str(col.get("name", "")).strip()
            if not col_name:
                continue

            entry: dict = {
                "table": table_id,
                "name": col_name,
                "type": str(col.get("type", "")).strip(),
                "description": shorten_description(str(col.get("description", ""))),
            }

            semantic_type = str(col.get("semantic_type", "")).strip()
            if semantic_type:
                entry["semantic_type"] = semantic_type

            keywords = col.get("keywords")
            if isinstance(keywords, list) and keywords:
                entry["keywords"] = [str(k).strip() for k in keywords if str(k).strip()][:8]

            select_expressions = col.get("select_expressions")
            if isinstance(select_expressions, list) and select_expressions:
                entry["select_expressions"] = [
                    str(e).strip() for e in select_expressions if str(e).strip()
                ]

            allowed_values = col.get("allowed_values")
            if isinstance(allowed_values, list) and allowed_values:
                entry["allowed_values"] = [
                    str(v).strip() for v in allowed_values if str(v).strip()
                ]

            columns.append(entry)

    return columns


def clean_table_metadata(table_meta_path: Path) -> list[dict]:
    """Clean table_metadata file: remove bloat keys, deduplicate joins."""
    if not table_meta_path.exists():
        print(f"  SKIP (not found): {table_meta_path}")
        return []

    payload = json.loads(table_meta_path.read_text(encoding="utf-8"))
    raw_docs = payload.get("documents", [])
    cleaned = []

    for doc in raw_docs:
        if not isinstance(doc, dict):
            continue
        tm = doc.get("table_metadata")
        if not isinstance(tm, dict):
            continue

        filtered_tm = {}
        for key in TABLE_METADATA_KEEP_KEYS:
            value = tm.get(key)
            if value not in (None, "", [], {}):
                filtered_tm[key] = value

        # Deduplicate relationships/joins
        if "relationships" in filtered_tm:
            rels = filtered_tm["relationships"]
            if isinstance(rels, list):
                seen = set()
                deduped = []
                for r in rels:
                    key = str(r).strip().lower()
                    if key not in seen:
                        seen.add(key)
                        deduped.append(r)
                filtered_tm["relationships"] = deduped

        cleaned.append({
            "id": str(doc.get("id", "")).strip(),
            "schema": str(doc.get("schema", "")).strip(),
            "name": str(doc.get("name", "")).strip(),
            "table_metadata": filtered_tm,
        })

    return cleaned


def extract_global_notes(vectored_path: Path) -> list[str]:
    """Extract global_reporting_notes from vectored metadata."""
    if not vectored_path.exists():
        return []
    payload = json.loads(vectored_path.read_text(encoding="utf-8"))
    notes = payload.get("global_reporting_notes")
    if isinstance(notes, list):
        return [str(n).strip() for n in notes if str(n).strip()]
    return []


def extract_known_tables(vectored_path: Path) -> list[str]:
    """Extract known_tables from vectored metadata."""
    if not vectored_path.exists():
        return []
    payload = json.loads(vectored_path.read_text(encoding="utf-8"))
    tables = payload.get("known_tables")
    if isinstance(tables, list):
        return [str(t).strip() for t in tables if str(t).strip()]
    return []


def process_agent(agent_id: str) -> None:
    """Process one agent: generate column_metadata + clean table_metadata."""
    vectored_path = METADATA_DIR / f"metadata_vectored_{agent_id}.json"
    table_meta_path = METADATA_DIR / f"table_metadata_{agent_id}.json"
    column_meta_path = METADATA_DIR / f"column_metadata_{agent_id}.json"

    print(f"\n{'='*60}")
    print(f"Processing agent: {agent_id}")
    print(f"{'='*60}")

    # --- Column metadata ---
    if not vectored_path.exists():
        print(f"  SKIP vectored (not found): {vectored_path}")
        return

    payload = json.loads(vectored_path.read_text(encoding="utf-8"))
    documents = payload.get("documents", [])
    if not isinstance(documents, list):
        print(f"  ERROR: no documents list in {vectored_path}")
        return

    columns = extract_columns(documents)

    column_metadata = {
        "metadata_version": 3,
        "agent_id": agent_id,
        "columns": columns,
    }
    column_meta_path.write_text(
        json.dumps(column_metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"  Created: {column_meta_path}")
    print(f"  Columns: {len(columns)}")

    # --- Clean table_metadata ---
    cleaned_docs = clean_table_metadata(table_meta_path)
    if cleaned_docs:
        cleaned_payload = {"documents": cleaned_docs}
        clean_path = METADATA_DIR / f"table_metadata_{agent_id}.clean.json"
        clean_path.write_text(
            json.dumps(cleaned_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"  Created cleaned table_metadata: {clean_path}")
        print(f"  Tables: {len(cleaned_docs)}")

        # Show size comparison
        original_size = table_meta_path.stat().st_size
        clean_size = clean_path.stat().st_size
        print(f"  Size: {original_size:,} -> {clean_size:,} bytes ({clean_size/original_size:.1%})")


def main() -> None:
    print("Split metadata script")
    print(f"Metadata dir: {METADATA_DIR}")

    for agent_id in AGENTS:
        try:
            process_agent(agent_id)
        except Exception as exc:
            print(f"  ERROR processing {agent_id}: {exc}")
            continue

    print(f"\n{'='*60}")
    print("Done. Review the generated files.")
    print("To replace table_metadata, rename *.clean.json -> *.json")


if __name__ == "__main__":
    main()
