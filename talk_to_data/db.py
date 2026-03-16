"""Oracle execution helpers with safe error handling."""

from __future__ import annotations

import importlib
import re
from typing import Any

import pandas as pd

from .config import AppConfig


class DatabaseExecutionError(RuntimeError):
    """Raised when database execution fails."""


def execute_sql(sql: str, requirements: dict[str, Any], config: AppConfig) -> pd.DataFrame:
    """Execute SQL on Oracle and return result as DataFrame."""
    missing = config.missing_oracle_env()
    if missing:
        joined = ", ".join(missing)
        raise DatabaseExecutionError(
            f"Oracle connection env vars are missing: {joined}"
        )

    module = _import_oracle_driver()
    bind_params = build_bind_params(sql, requirements)

    try:
        connection = module.connect(
            user=config.oracle_user,
            password=config.oracle_password,
            dsn=config.oracle_dsn,
        )
    except Exception as exc:  # pragma: no cover - depends on runtime driver
        raise DatabaseExecutionError(
            _sanitize_error(str(exc), config)
        ) from exc

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, bind_params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in (cursor.description or [])]
    except Exception as exc:  # pragma: no cover - depends on runtime database
        raise DatabaseExecutionError(_sanitize_error(str(exc), config)) from exc
    finally:
        try:
            connection.close()
        except Exception:
            pass

    return pd.DataFrame(rows, columns=columns)


def build_bind_params(sql: str, requirements: dict[str, Any]) -> dict[str, Any]:
    """Build bind variable dictionary based on SQL placeholders and extracted requirements."""
    placeholders = extract_placeholders(sql)
    if not placeholders:
        return {}
    return _resolve_bind_values(placeholders, requirements, strict=True)


def render_sql_for_display(sql: str, requirements: dict[str, Any]) -> str:
    """Render SQL for UI display with best-effort bind replacement."""
    if not sql:
        return ""
    placeholders = extract_placeholders(sql)
    if not placeholders:
        return sql
    if not isinstance(requirements, dict):
        return sql

    resolved = _resolve_bind_values(placeholders, requirements, strict=False)

    def repl(match: re.Match[str]) -> str:
        bind_name = match.group(1)
        if bind_name not in resolved:
            return match.group(0)
        return _to_sql_literal(resolved[bind_name])

    return re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", repl, sql)


def extract_placeholders(sql: str) -> list[str]:
    """Return ordered unique bind names from SQL."""
    names = re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql)
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        if name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def _import_oracle_driver() -> Any:
    for module_name in ("oracledb", "cx_Oracle"):
        try:
            return importlib.import_module(module_name)
        except ImportError:
            continue
    raise DatabaseExecutionError(
        "Oracle driver not installed. Install 'oracledb' or 'cx_Oracle'."
    )


def _pick_time_range(time_range: Any, key: str) -> Any:
    if isinstance(time_range, dict):
        value = time_range.get(key)
        if value not in (None, ""):
            return value
    return None


def _safe_row_limit(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 200
    return max(1, min(parsed, 5000))


def _resolve_bind_values(
    placeholders: list[str],
    requirements: dict[str, Any],
    *,
    strict: bool,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    report_period = requirements.get("report_period")
    time_range = requirements.get("time_range")
    time_granularity = str(requirements.get("time_granularity", "")).strip().lower()
    time_value = _normalize_digits(requirements.get("time_value"))

    for name in placeholders:
        lowered = name.lower()
        if lowered in ("n", "row_limit", "limit_n"):
            params[name] = _safe_row_limit(requirements.get("row_limit"))
            continue
        if lowered == "report_period":
            if report_period in (None, ""):
                _raise_or_skip(
                    strict,
                    "Missing value for bind variable :report_period.",
                )
                continue
            params[name] = report_period
            continue
        if lowered == "year_value":
            value = _resolve_year_value(
                requirements=requirements,
                time_granularity=time_granularity,
                time_value=time_value,
            )
            if value is None:
                _raise_or_skip(strict, "Missing value for bind variable :year_value.")
                continue
            params[name] = value
            continue
        if lowered == "date_value":
            value = _resolve_date_value(
                requirements=requirements,
                time_granularity=time_granularity,
                time_value=time_value,
            )
            if value is None:
                _raise_or_skip(strict, "Missing value for bind variable :date_value.")
                continue
            params[name] = value
            continue
        if lowered in ("start_date", "from_date", "date_start"):
            value = _pick_time_range(time_range, "start")
            if value is None:
                _raise_or_skip(strict, f"Missing value for bind variable :{name}.")
                continue
            params[name] = value
            continue
        if lowered in ("end_date", "to_date", "date_end"):
            value = _pick_time_range(time_range, "end")
            if value is None:
                _raise_or_skip(strict, f"Missing value for bind variable :{name}.")
                continue
            params[name] = value
            continue

        generic_value = requirements.get(name)
        if generic_value in (None, ""):
            _raise_or_skip(strict, f"Missing value for bind variable :{name}.")
            continue
        params[name] = generic_value

    return params


def _raise_or_skip(strict: bool, message: str) -> None:
    if strict:
        raise DatabaseExecutionError(message)


def _to_sql_literal(value: Any) -> str:
    text = str(value).strip()
    if not text or text.lower() == "none":
        return "NULL"
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return text
    escaped = text.replace("'", "''")
    return f"'{escaped}'"


def _resolve_year_value(
    *,
    requirements: dict[str, Any],
    time_granularity: str,
    time_value: str,
) -> str | None:
    direct = _normalize_digits(requirements.get("year_value"))
    if re.fullmatch(r"20\d{2}", direct):
        return direct
    if time_granularity == "year" and re.fullmatch(r"20\d{2}", time_value):
        return time_value
    return None


def _resolve_date_value(
    *,
    requirements: dict[str, Any],
    time_granularity: str,
    time_value: str,
) -> str | None:
    direct = _normalize_digits(requirements.get("date_value"))
    if re.fullmatch(r"20\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])", direct):
        return direct
    if time_granularity == "day" and re.fullmatch(
        r"20\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])",
        time_value,
    ):
        return time_value
    return None


def _normalize_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def _sanitize_error(message: str, config: AppConfig) -> str:
    sanitized = message
    for secret in (config.oracle_password, config.oracle_user, config.oracle_dsn):
        if secret:
            sanitized = sanitized.replace(secret, "<redacted>")
    return sanitized
