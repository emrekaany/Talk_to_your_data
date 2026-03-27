"""One-shot script to add missing FK/PK columns to uretim metadata files.

Automatically scans join definitions in metadata_vectored_uretim.json and adds
any join-key columns that are not already in the table's columns array.
Also mirrors additions into column_metadata_uretim.json.
"""
import json, os

BASE = os.path.join(os.path.dirname(__file__), "..", "metadata", "agents")


def _infer_type(col_name: str) -> str:
    """Infer Oracle column type from naming convention."""
    upper = col_name.upper()
    if upper.endswith("_ID"):
        return "NUMBER"
    if upper.endswith("_KODU") or upper.endswith("_KODU_AS400"):
        return "VARCHAR2"
    return "VARCHAR2"


def _find_missing_join_columns(docs: list[dict]) -> dict[str, list[dict]]:
    """For each table, return join columns missing from its columns array."""
    # Build map: table_id -> set of existing column names
    existing_map: dict[str, set[str]] = {}
    for doc in docs:
        tid = doc.get("id", "")
        existing_map[tid] = {c["name"] for c in doc.get("columns", [])}

    # Scan joins and collect missing columns with their partner info
    missing_map: dict[str, dict[str, dict]] = {}  # tid -> {col_name -> col_def}
    for doc in docs:
        tid = doc.get("id", "")
        existing = existing_map.get(tid, set())
        for j in doc.get("joins", []):
            if not isinstance(j, dict):
                continue
            lt = j.get("left_table", "")
            lc = j.get("left_column", "")
            rt = j.get("right_table", "")
            rc = j.get("right_column", "")
            raw = j.get("raw_condition", "")
            # If this table is on the left side and column is missing
            if lt == tid and lc and lc not in existing:
                missing_map.setdefault(tid, {})[lc] = {
                    "partner_table": rt,
                    "partner_column": rc,
                    "raw": raw,
                }
            # If this table is on the right side and column is missing
            if rt == tid and rc and rc not in existing:
                missing_map.setdefault(tid, {})[rc] = {
                    "partner_table": lt,
                    "partner_column": lc,
                    "raw": raw,
                }

    # Convert to column definitions
    result: dict[str, list[dict]] = {}
    for tid, cols in missing_map.items():
        col_list = []
        for col_name, info in sorted(cols.items()):
            col_list.append({
                "name": col_name,
                "type": _infer_type(col_name),
                "description": (
                    f"Join key kolonu. {info['raw']}"
                    if info["raw"]
                    else f"Join key: {tid}.{col_name} = {info['partner_table']}.{info['partner_column']}"
                ),
                "semantic_type": "Dimensional",
                "keywords": [col_name.replace("_", " ")],
            })
        result[tid] = col_list
    return result


def enrich_vectored():
    path = os.path.join(BASE, "metadata_vectored_uretim.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        docs = data
    elif isinstance(data, dict) and "documents" in data:
        docs = data["documents"]
    else:
        raise RuntimeError("Cannot find document list")

    additions = _find_missing_join_columns(docs)
    changes = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        tid = doc.get("id", "")
        if tid not in additions:
            continue
        existing = {c["name"] for c in doc.get("columns", [])}
        for col_def in additions[tid]:
            if col_def["name"] not in existing:
                doc.setdefault("columns", []).append(col_def)
                changes.append(f"{tid}.{col_def['name']}")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=6)
    print(f"metadata_vectored: added {len(changes)} columns: {changes}")
    return additions


def enrich_table_metadata(additions: dict[str, list[dict]]):
    path = os.path.join(BASE, "table_metadata_uretim.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "documents" in data:
        docs = data["documents"]
    elif isinstance(data, list):
        docs = data
    else:
        raise RuntimeError("Cannot find table list")

    changes = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        tid = doc.get("id", "")
        if tid not in additions:
            continue
        existing = {c["name"] for c in doc.get("columns", [])} if "columns" in doc else set()
        for col_def in additions[tid]:
            if col_def["name"] not in existing:
                doc.setdefault("columns", []).append(col_def)
                changes.append(f"{tid}.{col_def['name']}")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"table_metadata: added {len(changes)} columns: {changes}")


def enrich_column_metadata(additions: dict[str, list[dict]]):
    path = os.path.join(BASE, "column_metadata_uretim.json")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "columns" in data:
        cols = data["columns"]
    elif isinstance(data, list):
        cols = data
    else:
        raise RuntimeError("Unexpected format")

    existing = {(c.get("table", ""), c.get("name", "")) for c in cols}
    changes = []
    for tid, col_defs in sorted(additions.items()):
        for col_def in col_defs:
            key = (tid, col_def["name"])
            if key not in existing:
                entry = {"table": tid}
                entry.update(col_def)
                cols.append(entry)
                changes.append(f"{tid}.{col_def['name']}")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"column_metadata: added {len(changes)} columns: {changes}")


if __name__ == "__main__":
    additions = enrich_vectored()
    enrich_table_metadata(additions)
    enrich_column_metadata(additions)
    print("Done.")
