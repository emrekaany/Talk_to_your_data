"""Result-to-text summarization helpers."""

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd

from .llm_client import LLMClient, LLMError


def summarize_result_to_text(
    df: pd.DataFrame,
    *,
    user_request: str | None = None,
    sql: str | None = None,
    llm_client: LLMClient | None = None,
    llm_enabled: bool | None = None,
) -> str:
    """Create human-readable summary from result DataFrame."""
    heuristic_summary = _heuristic_summary(df)
    if not _llm_summarizer_enabled(llm_enabled):
        return heuristic_summary

    llm_summary = _summarize_with_llm(
        df,
        user_request=user_request,
        sql=sql,
        llm_client=llm_client,
        fallback_summary=heuristic_summary,
    )
    if llm_summary:
        return llm_summary
    return heuristic_summary


def _heuristic_summary(df: pd.DataFrame) -> str:
    row_count, col_count = df.shape
    if row_count == 0:
        return "Query returned 0 rows."

    lines = [
        f"Rows: {row_count}",
        f"Columns: {col_count}",
    ]

    if row_count > 2000:
        lines.append("Large result set detected. Summary uses only shape and first rows.")
        lines.append(f"First 5 rows preview:\n{df.head(5).to_string(index=False)}")
        return "\n".join(lines)

    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    text_cols = [col for col in df.columns if col not in numeric_cols]

    if numeric_cols:
        for col in numeric_cols[:3]:
            series = df[col].dropna()
            if series.empty:
                continue
            total = float(series.sum())
            average = float(series.mean())
            lines.append(
                f"{col}: sum={total:,.2f}, avg={average:,.2f}, min={series.min():,.2f}, max={series.max():,.2f}"
            )

    if text_cols:
        col = text_cols[0]
        top_values = (
            df[col]
            .astype(str)
            .value_counts(dropna=False)
            .head(5)
            .to_dict()
        )
        pretty = ", ".join(f"{k}={v}" for k, v in top_values.items())
        lines.append(f"Top values in {col}: {pretty}")

    lines.append("First 5 rows preview:")
    lines.append(df.head(5).to_string(index=False))
    return "\n".join(lines)


def _summarize_with_llm(
    df: pd.DataFrame,
    *,
    user_request: str | None,
    sql: str | None,
    llm_client: LLMClient | None,
    fallback_summary: str,
) -> str | None:
    if llm_client is None:
        return None

    profile = _build_result_profile(df)
    rows_preview = dataframe_to_records(df, limit=20)
    rows_json = json.dumps(rows_preview, ensure_ascii=True)
    if len(rows_json) > 7000:
        rows_json = f"{rows_json[:7000]}..."

    prompt = (
        "Summarize this SQL result for a business user.\n"
        "Rules:\n"
        "- Output plain text only.\n"
        "- Mention row count, key metrics, and top patterns.\n"
        "- Mention notable caveats if data seems partial or sparse.\n"
        "- Keep it concise and practical.\n\n"
        f"User request:\n{user_request or ''}\n\n"
        f"Executed SQL:\n{sql or ''}\n\n"
        f"Result profile:\n{json.dumps(profile, ensure_ascii=True)}\n\n"
        f"First rows (JSON):\n{rows_json}\n\n"
        f"Fallback heuristic summary:\n{fallback_summary}"
    )

    try:
        text = llm_client.chat(
            "You are a senior analytics engineer writing clear result summaries.",
            prompt,
            temperature=0.0,
            max_tokens=500,
        ).strip()
    except LLMError:
        return None
    if not text:
        return None
    return _strip_fence(text)


def _build_result_profile(df: pd.DataFrame) -> dict[str, Any]:
    row_count, col_count = df.shape
    profile: dict[str, Any] = {
        "row_count": row_count,
        "column_count": col_count,
        "columns": [str(col) for col in df.columns[:25]],
        "numeric_stats": {},
    }
    for col in df.columns[:10]:
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        series = df[col].dropna()
        if series.empty:
            continue
        profile["numeric_stats"][str(col)] = {
            "sum": float(series.sum()),
            "avg": float(series.mean()),
            "min": float(series.min()),
            "max": float(series.max()),
        }
    return profile


def _llm_summarizer_enabled(explicit_flag: bool | None) -> bool:
    if explicit_flag is not None:
        return explicit_flag
    raw = os.getenv("LLM_SUMMARIZER_ENABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped[3:]
        if stripped.startswith("text"):
            stripped = stripped[4:]
        stripped = stripped.rstrip("`").strip()
    return stripped


def dataframe_to_records(df: pd.DataFrame, limit: int = 500) -> list[dict[str, Any]]:
    """Return a preview-safe list of row records."""
    if df.empty:
        return []
    return df.head(limit).to_dict(orient="records")
