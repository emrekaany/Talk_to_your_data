"""Result-to-text summarization helpers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Any

import pandas as pd

from .llm_client import LLMClient, LLMError


_NO_SUMMARY_TEXT = "Bu veriye turkce aciklama uretilemiyor."
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_MAX_SUMMARY_SENTENCE = 4
_CHART_TYPES = {"bar", "line", "scatter", "pie", "none"}
_AGGREGATIONS = {"sum", "avg", "count", "none"}
_SORT_ORDERS = {"asc", "desc", "none"}


@dataclass
class ResultInterpretation:
    """Structured result interpretation payload."""

    summary_text: str
    chart_plan: dict[str, Any]
    llm_used: bool
    chart_render_enabled: bool
    summary_mode: str
    fallback_reason: str | None
    validation_errors: list[str]


def summarize_result_to_text(
    df: pd.DataFrame,
    *,
    user_request: str | None = None,
    sql: str | None = None,
    metadata_used: dict[str, Any] | None = None,
    llm_client: LLMClient | None = None,
    llm_enabled: bool | None = None,
    chart_render_enabled: bool = False,
) -> str:
    """Create human-readable summary from result DataFrame."""
    interpreted = summarize_result(
        df,
        user_request=user_request,
        sql=sql,
        metadata_used=metadata_used,
        llm_client=llm_client,
        llm_enabled=llm_enabled,
        chart_render_enabled=chart_render_enabled,
    )
    return interpreted.summary_text


def summarize_result(
    df: pd.DataFrame,
    *,
    user_request: str | None = None,
    sql: str | None = None,
    metadata_used: dict[str, Any] | None = None,
    llm_client: LLMClient | None = None,
    llm_enabled: bool | None = None,
    chart_render_enabled: bool = False,
) -> ResultInterpretation:
    """
    Create Turkish result summary and chart plan.

    Chart rendering can be disabled while still emitting a chart plan structure.
    """
    heuristic_summary = _heuristic_summary_tr(df)
    disabled_reason = "Grafik uretimi konfigrasyonda devre disi."

    if not _llm_summarizer_enabled(llm_enabled):
        return _heuristic_result(
            heuristic_summary=heuristic_summary,
            chart_render_enabled=chart_render_enabled,
            chart_disabled_reason=disabled_reason,
            fallback_reason="LLM summarizer disabled by configuration.",
        )

    if llm_client is None:
        return _heuristic_result(
            heuristic_summary=heuristic_summary,
            chart_render_enabled=chart_render_enabled,
            chart_disabled_reason=disabled_reason,
            fallback_reason="LLM client unavailable.",
        )

    payload = _summarize_with_llm(
        df,
        user_request=user_request,
        sql=sql,
        metadata_used=metadata_used,
        llm_client=llm_client,
        fallback_summary=heuristic_summary,
    )
    if payload is None:
        return _heuristic_result(
            heuristic_summary=heuristic_summary,
            chart_render_enabled=chart_render_enabled,
            chart_disabled_reason=disabled_reason,
            fallback_reason="LLM summary fallback due LLM error or invalid JSON.",
        )

    summary_text = _normalize_summary_text(
        payload.get("summary_tr"),
        fallback=heuristic_summary,
    )
    chart_plan, validation_errors = validate_chart_plan(
        payload.get("chart_plan"),
        df,
    )
    if not chart_render_enabled:
        chart_plan = _force_chart_disabled(chart_plan, disabled_reason)

    return ResultInterpretation(
        summary_text=summary_text,
        chart_plan=chart_plan,
        llm_used=True,
        chart_render_enabled=chart_render_enabled,
        summary_mode="llm",
        fallback_reason=None,
        validation_errors=validation_errors,
    )


def _heuristic_summary_tr(df: pd.DataFrame) -> str:
    row_count, col_count = df.shape
    if row_count == 0:
        return _NO_SUMMARY_TEXT

    sentences: list[str] = [
        f"Sorgu sonucu {row_count} satir ve {col_count} kolon dondurdu.",
    ]

    numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    text_cols = [col for col in df.columns if col not in numeric_cols]

    if numeric_cols:
        col = numeric_cols[0]
        series = df[col].dropna()
        if not series.empty:
            sentences.append(
                f"{col} alani icin toplam {float(series.sum()):,.2f}, ortalama {float(series.mean()):,.2f}."
            )

    if text_cols:
        col = text_cols[0]
        top_values = df[col].astype(str).value_counts(dropna=False).head(3).to_dict()
        if top_values:
            sample = ", ".join(f"{key}={value}" for key, value in top_values.items())
            sentences.append(f"{col} dagiliminda en sik degerler: {sample}.")

    sentences.append("Bu ozet mevcut sonuc kumesi uzerinden uretilmistir.")
    return _limit_sentences(" ".join(sentences), _MAX_SUMMARY_SENTENCE)


def _summarize_with_llm(
    df: pd.DataFrame,
    *,
    user_request: str | None,
    sql: str | None,
    metadata_used: dict[str, Any] | None,
    llm_client: LLMClient,
    fallback_summary: str,
) -> dict[str, Any] | None:
    profile = _build_result_profile(df)
    rows_preview = dataframe_to_records(df, limit=20)
    rows_json = json.dumps(rows_preview, ensure_ascii=True)
    if len(rows_json) > 7000:
        rows_json = f"{rows_json[:7000]}..."

    metadata_summary = _build_metadata_summary(metadata_used)
    schema = {
        "summary_tr": "string (max 4 cumle)",
        "chart_plan": {
            "draw_chart": "boolean",
            "chart_type": "bar|line|scatter|pie|none",
            "x": "string|null",
            "y": "string|null",
            "aggregation": "sum|avg|count|none",
            "top_n": "integer|null",
            "sort": "asc|desc|none",
            "title_tr": "string",
            "reason_tr": "string",
        },
    }
    prompt = (
        "Gorev: Calistirilmis SQL sonucunu turkce yorumla ve grafik plani oner.\n"
        "Kurallar:\n"
        "- Sadece verilen veri ve baglami kullan, tahmin yapma.\n"
        "- Veri yetersizse summary_tr alanina tam olarak "
        f"'{_NO_SUMMARY_TEXT}' yaz.\n"
        "- Markdown kullanma.\n"
        "- Sadece gecerli JSON dondur.\n"
        "- summary_tr en fazla 4 cumle olsun.\n"
        "- Grafik kodu dondurme; yalnizca chart_plan nesnesi dondur.\n\n"
        f"JSON semasi:\n{json.dumps(schema, ensure_ascii=True)}\n\n"
        f"Kullanici istegi:\n{user_request or ''}\n\n"
        f"Calistirilan SQL:\n{sql or ''}\n\n"
        f"Ilgili metadata ozeti:\n{json.dumps(metadata_summary, ensure_ascii=True)}\n\n"
        f"Sonuc profili:\n{json.dumps(profile, ensure_ascii=True)}\n\n"
        f"Ilk satirlar (JSON):\n{rows_json}\n\n"
        f"Fallback ozet:\n{fallback_summary}"
    )

    try:
        text = llm_client.chat(
            "Sen kidemli bir veri analisti ve is zekasi danismanisin. Sadece JSON dondur.",
            prompt,
            temperature=0.0,
            max_tokens=900,
        ).strip()
    except LLMError:
        return None

    if not text:
        return None

    return _parse_json_payload(text)


def _build_metadata_summary(metadata_used: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata_used, dict):
        return {}

    summary: dict[str, Any] = {
        "dialect": str(metadata_used.get("dialect", "")).strip(),
        "mandatory_rules": _as_string_list(metadata_used.get("mandatory_rules"))[:12],
        "guardrails": _as_string_list(metadata_used.get("guardrails"))[:12],
        "tables": [],
    }

    relevant = metadata_used.get("relevant_items")
    if isinstance(relevant, list):
        for item in relevant[:8]:
            if not isinstance(item, dict):
                continue
            row: dict[str, Any] = {
                "table": str(item.get("table", "")).strip(),
                "columns": [],
            }
            raw_columns = item.get("columns")
            if isinstance(raw_columns, list):
                row["columns"] = [
                    str(col.get("name", "")).strip()
                    for col in raw_columns[:12]
                    if isinstance(col, dict) and str(col.get("name", "")).strip()
                ]
            summary["tables"].append(row)
    return summary


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


def validate_chart_plan(
    value: Any,
    df: pd.DataFrame,
) -> tuple[dict[str, Any], list[str]]:
    """
    Validate and normalize chart plan with DataFrame-aware checks.

    Rules:
    - chart_type in {bar,line,scatter,pie,none}
    - draw_chart=true requires chart_type!=none
    - bar/line/scatter/pie require x column existing in df
    - sum/avg require y column existing and numeric
    - count allows optional y; if provided it must exist
    - top_n positive int and <= 200
    """
    fallback_reason = "LLM chart plan verisi alinmadi."
    if not isinstance(value, dict):
        return _disabled_chart_plan(fallback_reason), [fallback_reason]

    errors: list[str] = []
    columns = {str(col) for col in df.columns}

    chart_type = str(value.get("chart_type", "none")).strip().lower()
    if chart_type not in _CHART_TYPES:
        errors.append(
            f"Invalid chart_type '{chart_type}'. Allowed: {', '.join(sorted(_CHART_TYPES))}."
        )
        chart_type = "none"

    aggregation = str(value.get("aggregation", "none")).strip().lower()
    if aggregation not in _AGGREGATIONS:
        errors.append(
            f"Invalid aggregation '{aggregation}'. Allowed: {', '.join(sorted(_AGGREGATIONS))}."
        )
        aggregation = "none"

    sort = str(value.get("sort", "none")).strip().lower()
    if sort not in _SORT_ORDERS:
        errors.append(
            f"Invalid sort '{sort}'. Allowed: {', '.join(sorted(_SORT_ORDERS))}."
        )
        sort = "none"

    draw_chart = bool(value.get("draw_chart", False))
    if draw_chart and chart_type == "none":
        errors.append("draw_chart=true requires chart_type other than 'none'.")

    x = _optional_text(value.get("x"))
    y = _optional_text(value.get("y"))

    if chart_type in {"bar", "line", "scatter", "pie"}:
        if not x:
            errors.append(f"{chart_type} chart requires x column.")
        elif x not in columns:
            errors.append(f"x column '{x}' was not found in result columns.")

    if aggregation in {"sum", "avg"}:
        if not y:
            errors.append(f"aggregation='{aggregation}' requires y column.")
        elif y not in columns:
            errors.append(f"y column '{y}' was not found in result columns.")
        elif not pd.api.types.is_numeric_dtype(df[y]):
            errors.append(
                f"y column '{y}' must be numeric for aggregation '{aggregation}'."
            )
    elif aggregation == "count" and y and y not in columns:
        errors.append(f"Optional count y column '{y}' was not found in result columns.")

    top_n_raw = value.get("top_n")
    top_n: int | None = None
    if top_n_raw not in (None, ""):
        try:
            parsed = int(top_n_raw)
        except (TypeError, ValueError):
            errors.append("top_n must be an integer.")
        else:
            if parsed <= 0:
                errors.append("top_n must be greater than zero.")
            elif parsed > 200:
                errors.append("top_n cannot exceed 200.")
                top_n = 200
            else:
                top_n = parsed

    plan = {
        "draw_chart": draw_chart,
        "chart_type": chart_type,
        "x": x,
        "y": y,
        "aggregation": aggregation,
        "top_n": top_n,
        "sort": sort,
        "title_tr": str(value.get("title_tr", "")).strip() or "Grafik plani",
        "reason_tr": str(value.get("reason_tr", "")).strip() or fallback_reason,
    }

    if errors:
        reason = "; ".join(errors)
        return _disabled_chart_plan(reason), errors

    if not plan["draw_chart"]:
        reason = str(plan.get("reason_tr", "")).strip() or "draw_chart is false."
        return _disabled_chart_plan(reason), []

    return plan, []


def _heuristic_result(
    *,
    heuristic_summary: str,
    chart_render_enabled: bool,
    chart_disabled_reason: str,
    fallback_reason: str,
) -> ResultInterpretation:
    return ResultInterpretation(
        summary_text=heuristic_summary,
        chart_plan=_disabled_chart_plan(chart_disabled_reason),
        llm_used=False,
        chart_render_enabled=chart_render_enabled,
        summary_mode="heuristic",
        fallback_reason=fallback_reason,
        validation_errors=[],
    )


def _disabled_chart_plan(reason: str) -> dict[str, Any]:
    return {
        "draw_chart": False,
        "chart_type": "none",
        "x": None,
        "y": None,
        "aggregation": "none",
        "top_n": None,
        "sort": "none",
        "title_tr": "Grafik devre disi",
        "reason_tr": str(reason).strip() or "Grafik devre disi.",
    }


def _force_chart_disabled(chart_plan: dict[str, Any], reason: str) -> dict[str, Any]:
    forced = dict(chart_plan)
    forced["draw_chart"] = False
    forced["chart_type"] = "none"
    existing = str(forced.get("reason_tr", "")).strip()
    if existing:
        forced["reason_tr"] = f"{existing} {reason}".strip()
    else:
        forced["reason_tr"] = reason
    return forced


def _optional_text(value: Any) -> str | None:
    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    return text


def _normalize_summary_text(value: Any, *, fallback: str) -> str:
    text = _strip_fence(str(value or "")).strip()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[#*_`]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        text = fallback
    return _limit_sentences(text, _MAX_SUMMARY_SENTENCE)


def _limit_sentences(text: str, max_sentences: int) -> str:
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    if not cleaned:
        return _NO_SUMMARY_TEXT
    parts = [part.strip() for part in _SENTENCE_SPLIT.split(cleaned) if part.strip()]
    if len(parts) <= max_sentences:
        return cleaned
    return " ".join(parts[:max_sentences])


def _parse_json_payload(raw: str) -> dict[str, Any] | None:
    text = _strip_fence(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _llm_summarizer_enabled(explicit_flag: bool | None) -> bool:
    if explicit_flag is not None:
        return explicit_flag
    raw = os.getenv("LLM_SUMMARIZER_ENABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _strip_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z0-9_]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped.strip()


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


def dataframe_to_records(df: pd.DataFrame, limit: int = 500) -> list[dict[str, Any]]:
    """Return a preview-safe list of row records."""
    if df.empty:
        return []
    return df.head(limit).to_dict(orient="records")
