"""Gradio app for Talk to Your Data."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import gradio as gr

from talk_to_data.db import render_sql_for_display
from talk_to_data.pipeline import PipelineError, TalkToDataService


logger = logging.getLogger(__name__)

SERVICE = TalkToDataService()
DEFAULT_ORACLE_JDBC_URL = "jdbc:oracle:thin:@//10.1.24.184:80/ODSPROD"
DEFAULT_ORACLE_USERNAME = "oeus_ky4642"
DEFAULT_ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")


APP_CSS = """
:root {
  --ink: #12253d;
  --ink-soft: #3d4e63;
  --surface: #f4f8fb;
  --surface-strong: #ffffff;
  --accent: #0f766e;
  --warn: #9a3412;
}
.gradio-container {
  font-family: "IBM Plex Sans", "Trebuchet MS", sans-serif;
  background: radial-gradient(circle at 20% 10%, #e7f4ff 0%, #f4f8fb 35%, #eef5ea 100%);
}
#panel {
  background: var(--surface-strong);
  border: 1px solid #dbe5ef;
  border-radius: 14px;
  box-shadow: 0 10px 28px rgba(17, 37, 56, 0.07);
}
#app-title h1, #app-title h2, #app-title h3, #app-title p {
  color: var(--ink);
}
"""

_PARALLEL_CHOICES = ["Paralelsiz", "Paralel 2", "Paralel 4"]
_PARALLEL_MAP: dict[str, int] = {"Paralelsiz": 0, "Paralel 2": 2, "Paralel 4": 4}


def _parse_parallel_level(label: str | None) -> int:
    """Return Oracle PARALLEL degree from UI label."""
    return _PARALLEL_MAP.get(str(label or "").strip(), 0)


def _empty_candidate_outputs() -> tuple[str, ...]:
    return ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "")


def _suggestion_markdown(suggestion_text: str, suggested_questions: list[str]) -> str:
    """Format suggestion as Markdown for display."""
    if not suggestion_text and not suggested_questions:
        return ""
    lines = ["### Bunu mu sormak istediniz? / Did you mean?\n"]
    if suggestion_text:
        lines.append(f"**Neden:** {suggestion_text}\n")
    if suggested_questions:
        lines.append("**Onerilen sorular:**")
        for idx, q in enumerate(suggested_questions, start=1):
            lines.append(f"{idx}. {q}")
    return "\n".join(lines)


def _load_agent_choices() -> tuple[list[tuple[str, str]], str | None, str]:
    try:
        agents = SERVICE.list_agents()
    except PipelineError as exc:
        return [], None, f"Agent registry unavailable: {exc}"

    choices: list[tuple[str, str]] = []
    default_value: str | None = None
    for index, agent in enumerate(agents):
        agent_id = str(agent.get("id", "")).strip()
        label = str(agent.get("label", "")).strip() or agent_id
        if not agent_id:
            continue
        choices.append((f"{label} ({agent_id})", agent_id))
        if index == 0:
            default_value = agent_id

    if not choices:
        return [], None, "Agent registry has no agents."
    return choices, default_value, ""


def generate_sql_options(
    user_request: str,
    agent_id: str | None = None,
    jdbc_url: str = "",
    username: str = "",
    password: str = "",
    use_all_metadata: bool = False,
    parallel_level: str = "Paralelsiz",
) -> tuple[Any, ...]:
    request = user_request.strip()
    if not request:
        c = _empty_candidate_outputs()
        return (
            "Please enter a natural-language request.",
            *c,
            gr.update(choices=[], value=None),
            None,
            "",
            [],
            "",
            "",
            gr.update(value=None),
            "",
            "",
        )

    try:
        context = SERVICE.prepare_candidates(
            request, agent_id=agent_id, use_all_metadata=use_all_metadata,
        )
    except PipelineError as exc:
        c = _empty_candidate_outputs()
        return (
            f"Generation failed: {exc}",
            *c,
            gr.update(choices=[], value=None),
            None,
            "",
            [],
            "",
            "",
            gr.update(value=None),
            "",
            "",
        )
    except Exception as exc:
        logger.exception("Unexpected error in generate_sql_options")
        c = _empty_candidate_outputs()
        return (
            f"Unexpected error: {exc}",
            *c,
            gr.update(choices=[], value=None),
            None,
            "",
            [],
            "",
            "",
            gr.update(value=None),
            "",
            "",
        )

    has_suggestion = bool(context.get("has_suggestion", False))
    suggestion_text = str(context.get("suggestion_text", "")).strip()
    suggested_questions = context.get("suggested_questions", [])
    if not isinstance(suggested_questions, list):
        suggested_questions = []
    suggestion_md = _suggestion_markdown(suggestion_text, suggested_questions)
    first_suggestion = suggested_questions[0] if suggested_questions else ""

    candidates = context.get("candidates", [])
    if not isinstance(candidates, list) or len(candidates) != 3:
        c = _empty_candidate_outputs()
        status = "Generation could not produce valid SQL candidates."
        if has_suggestion:
            status = f"SQL olusturulamadi. Asagidaki onerilere bakin.\n{suggestion_text}"
        return (
            status,
            *c,
            gr.update(choices=[], value=None),
            None,
            "",
            [],
            "",
            "",
            gr.update(value=None),
            suggestion_md,
            first_suggestion,
        )

    requirements = context.get("requirements")
    if not isinstance(requirements, dict):
        requirements = {}

    radio_choices: list[tuple[str, str]] = []
    option_values: list[tuple[str, str, str, str, str]] = []
    for idx, candidate in enumerate(candidates, start=1):
        sql = str(candidate.get("sql", "")).strip()
        final_sql = render_sql_for_display(sql, requirements)
        desc = str(candidate.get("description", "")).strip()
        rationale = str(candidate.get("rationale_short", "")).strip()
        risk = str(candidate.get("risk_notes", "")).strip()
        candidate_id = str(candidate.get("id", f"option_{idx}"))
        option_values.append((final_sql, "", desc, rationale, risk))
        radio_choices.append((f"{candidate_id}: {rationale or 'Candidate SQL'}", candidate_id))

    selected_agent_label = str(context.get("agent_label", "")).strip()
    selected_agent_id = str(context.get("agent_id", "")).strip()
    recommended_candidate_id = str(context.get("recommended_candidate_id", "")).strip()
    selection_mode = str(context.get("selection_mode", "fallback")).strip() or "fallback"
    llm_usage = context.get("llm_usage")
    llm_call_count = 0
    if isinstance(llm_usage, dict):
        try:
            llm_call_count = int(llm_usage.get("total_calls", 0))
        except (TypeError, ValueError):
            llm_call_count = 0
    if not recommended_candidate_id and radio_choices:
        recommended_candidate_id = radio_choices[0][1]
    warning = str(context.get("warning", "")).strip()
    if warning:
        status = (
            f"\u26a0 {warning}\n"
            f"Generated 3 SQL options for agent '{selected_agent_label or selected_agent_id}'. "
            f"Auto-selected: {recommended_candidate_id or 'n/a'} ({selection_mode}). "
            f"Run folder: {context.get('run_dir')} "
            f"(LLM mode: {context.get('llm_mode')}, request LLM calls: {llm_call_count})."
        )
    else:
        status = (
            f"Generated 3 SQL options for agent '{selected_agent_label or selected_agent_id}'. "
            f"Auto-selected: {recommended_candidate_id or 'n/a'} ({selection_mode}). "
            f"Run folder: {context.get('run_dir')} "
            f"(LLM mode: {context.get('llm_mode')}, request LLM calls: {llm_call_count})."
        )
    selected_default = recommended_candidate_id or (radio_choices[0][1] if radio_choices else None)
    option_1 = option_values[0]
    option_2 = option_values[1]
    option_3 = option_values[2]

    run_status_text = ""
    result_preview: Any = []
    result_summary = ""
    result_chart_plan = ""
    result_file_path: str | None = None
    if selected_default:
        connection = {
            "dsn": jdbc_url.strip(),
            "user": username.strip(),
            "password": password,
        }
        parallel_hint = _parse_parallel_level(parallel_level)
        try:
            auto_result = SERVICE.execute_selected_candidate(
                context,
                selected_default,
                connection=connection,
                parallel_hint=parallel_hint,
                defer_llm_summary=True,
            )
            result_preview = auto_result.dataframe.head(500)
            result_summary = auto_result.summary
            result_chart_plan = _format_chart_plan(auto_result.chart_plan)
            result_file_path = str(auto_result.excel_path)
            llm_status = _llm_summary_status(
                auto_result.summary_mode,
                auto_result.fallback_reason,
            )
            run_status_text = (
                f"Auto-selected SQL '{selected_default}' executed successfully. "
                f"Rows: {len(auto_result.dataframe)} | "
                f"LLM summary: {llm_status}"
            )
            final_sql = _find_candidate_display_sql(context, selected_default)
            if final_sql:
                run_status_text += f"\n\nFinal SQL:\n{final_sql}"
            if auto_result.fallback_reason:
                run_status_text += f"\nFallback reason: {auto_result.fallback_reason}"
            if auto_result.validation_errors:
                run_status_text += (
                    "\nChart plan validation errors: "
                    + "; ".join(auto_result.validation_errors)
                )
        except PipelineError as exc:
            sql_text = _find_candidate_display_sql(context, selected_default)
            run_status_text = f"Auto-run failed for '{selected_default}': {exc}"
            if sql_text:
                run_status_text += f"\n\nFinal SQL:\n{sql_text}"
        except Exception as exc:
            logger.exception("Unexpected error during auto-run in generate_sql_options")
            run_status_text = f"Auto-run unexpected error for '{selected_default}': {exc}"
    else:
        run_status_text = "Auto-run skipped: recommended SQL option was not resolved."

    if result_file_path and not Path(result_file_path).exists():
        logger.warning("Excel file missing after write: %s", result_file_path)
        result_file_path = None

    return (
        status,
        option_1[0],
        option_1[1],
        option_1[2],
        option_1[3],
        option_1[4],
        option_2[0],
        option_2[1],
        option_2[2],
        option_2[3],
        option_2[4],
        option_3[0],
        option_3[1],
        option_3[2],
        option_3[3],
        option_3[4],
        gr.update(choices=radio_choices, value=selected_default),
        context,
        run_status_text,
        result_preview,
        result_summary,
        result_chart_plan,
        result_file_path,
        suggestion_md,
        first_suggestion,
    )


def run_selected_sql(
    selected_option: str,
    context: dict[str, Any] | None,
    jdbc_url: str,
    username: str,
    password: str,
    parallel_level: str = "Paralelsiz",
) -> tuple[str, Any, str, str, str | None]:
    if not context:
        return (
            "No SQL context found. First click 'Generate SQL Options'.",
            [],
            "",
            "",
            gr.update(value=None),
        )

    selected = selected_option.strip()
    if not selected:
        selected = str(context.get("recommended_candidate_id", "")).strip()
    if not selected:
        return ("No SQL option resolved to run.", [], "", "", gr.update(value=None))

    connection = {
        "dsn": jdbc_url.strip(),
        "user": username.strip(),
        "password": password,
    }

    try:
        result = SERVICE.execute_selected_candidate(
            context,
            selected,
            connection=connection,
            parallel_hint=_parse_parallel_level(parallel_level),
        )
    except PipelineError as exc:
        sql_text = _find_candidate_display_sql(context, selected)
        error_message = f"Run failed: {exc}"
        if sql_text:
            error_message += f"\n\nFinal SQL:\n{sql_text}"
        return (error_message, [], "", "", gr.update(value=None))
    except Exception as exc:
        logger.exception("Unexpected error in run_selected_sql")
        return (f"Unexpected error: {exc}", [], "", "", gr.update(value=None))

    preview = result.dataframe.head(500)
    llm_status = _llm_summary_status(result.summary_mode, result.fallback_reason)
    status = (
        f"Query executed successfully. Rows: {len(result.dataframe)} | "
        f"LLM summary: {llm_status}"
    )
    final_sql = _find_candidate_display_sql(context, selected)
    if final_sql:
        status += f"\n\nFinal SQL:\n{final_sql}"
    if result.fallback_reason:
        status += f"\nFallback reason: {result.fallback_reason}"
    if result.validation_errors:
        status += (
            "\nChart plan validation errors: "
            + "; ".join(result.validation_errors)
        )
    chart_plan_text = _format_chart_plan(result.chart_plan)
    excel_path = str(result.excel_path)
    if not Path(excel_path).exists():
        logger.warning("Excel file missing after write: %s", excel_path)
        excel_path = None
    return (
        status,
        preview,
        result.summary,
        chart_plan_text,
        excel_path,
    )


def _complete_deferred_summary(
    context: dict[str, Any] | None,
    result_preview: Any,
    current_summary: str,
    current_chart_plan: str,
) -> tuple[str, str]:
    """Run deferred LLM summarization after the dataframe is already displayed."""
    if not isinstance(context, dict):
        return current_summary, current_chart_plan
    recommended = str(context.get("recommended_candidate_id", "")).strip()
    if not recommended:
        return current_summary, current_chart_plan
    try:
        import pandas as pd
        if isinstance(result_preview, pd.DataFrame) and not result_preview.empty:
            df = result_preview
        else:
            return current_summary, current_chart_plan
        interpreted = SERVICE.complete_deferred_summary(context, df, recommended)
        if interpreted is None:
            return current_summary, current_chart_plan
        summary = interpreted.summary_text or current_summary
        chart_plan = _format_chart_plan(interpreted.chart_plan) or current_chart_plan
        return summary, chart_plan
    except Exception as exc:
        logger.debug("Deferred summary failed: %s", exc)
        return current_summary, current_chart_plan


def _find_candidate_sql(context: dict[str, Any], candidate_id: str) -> str:
    candidates = context.get("candidates")
    if not isinstance(candidates, list):
        return ""
    for candidate in candidates:
        if isinstance(candidate, dict) and str(candidate.get("id")) == candidate_id:
            return str(candidate.get("sql", "")).strip()
    return ""


def _find_candidate_display_sql(context: dict[str, Any], candidate_id: str) -> str:
    sql = _find_candidate_sql(context, candidate_id)
    if not sql:
        return ""
    requirements = context.get("requirements")
    if not isinstance(requirements, dict):
        requirements = {}
    return render_sql_for_display(sql, requirements)


def _llm_summary_status(summary_mode: str, fallback_reason: str | None) -> str:
    mode = str(summary_mode).strip().lower()
    if mode == "llm":
        return "enabled"
    reason = str(fallback_reason or "").strip().lower()
    if "disabled" in reason:
        return "disabled"
    return "fallback"


def _format_chart_plan(chart_plan: dict[str, Any] | None) -> str:
    if not isinstance(chart_plan, dict):
        return ""
    try:
        return json.dumps(chart_plan, ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        return ""


def build_app() -> gr.Blocks:
    with gr.Blocks(title="Talk to Your Data") as demo:
        context_state = gr.State(value=None)
        suggestion_state = gr.State(value="")
        agent_choices, default_agent, agent_warning = _load_agent_choices()

        with gr.Column(elem_id="panel"):
            gr.Markdown(
                """
                ## Talk to Your Data
                Generate 3 safe Oracle SQL options, auto-select the best one, and execute it immediately.
                """,
                elem_id="app-title",
            )
            with gr.Accordion("Oracle Connection", open=False):
                db_type = gr.Dropdown(
                    label="Database",
                    choices=["Oracle"],
                    value="Oracle",
                    interactive=False,
                )
                jdbc_url_box = gr.Textbox(
                    label="JDBC URL Template",
                    value=DEFAULT_ORACLE_JDBC_URL,
                    lines=1,
                )
                oracle_user_box = gr.Textbox(
                    label="Username",
                    value=DEFAULT_ORACLE_USERNAME,
                    lines=1,
                )
                oracle_password_box = gr.Textbox(
                    label="Password",
                    value=DEFAULT_ORACLE_PASSWORD,
                    type="password",
                    lines=1,
                )
            agent_selector = gr.Dropdown(
                label="Agent",
                choices=agent_choices,
                value=default_agent,
                allow_custom_value=False,
            )
            if agent_warning:
                gr.Markdown(f"Agent setup warning: {agent_warning}")
            request_box = gr.Textbox(
                label="Your request",
                placeholder="Example: 202501 donemi icin BRANS_KODU bazinda toplam PRIM_TL getir.",
                lines=4,
            )
            all_metadata_checkbox = gr.Checkbox(
                label="All Metadata (retrieval bypass)",
                value=False,
            )
            parallel_selector = gr.Radio(
                label="SQL Paralellik Seviyesi",
                choices=_PARALLEL_CHOICES,
                value="Paralelsiz",
            )
            generate_btn = gr.Button("Generate SQL Options", variant="primary")
            generation_status = gr.Markdown(label="Generation status")
            suggestion_box = gr.Markdown(value="", elem_id="suggestion-box")
            resubmit_btn = gr.Button(
                "Oneriyi kullan / Use suggestion",
                variant="secondary",
                visible=True,
            )

        with gr.Column(elem_id="panel"):
            gr.Markdown("### Did you mean this?")

            with gr.Accordion("Option 1", open=False):
                sql_1 = gr.Code(label="Final SQL (Bind-Resolved Preview)", language="sql")
                resolved_sql_1 = gr.Code(
                    label="Unused",
                    language="sql",
                    visible=False,
                )
                desc_1 = gr.Textbox(label="Explanation", lines=6)
                rationale_1 = gr.Textbox(label="Rationale", lines=2)
                risk_1 = gr.Textbox(label="Risk notes", lines=2)

            with gr.Accordion("Option 2", open=False):
                sql_2 = gr.Code(label="Final SQL (Bind-Resolved Preview)", language="sql")
                resolved_sql_2 = gr.Code(
                    label="Unused",
                    language="sql",
                    visible=False,
                )
                desc_2 = gr.Textbox(label="Explanation", lines=6)
                rationale_2 = gr.Textbox(label="Rationale", lines=2)
                risk_2 = gr.Textbox(label="Risk notes", lines=2)

            with gr.Accordion("Option 3", open=False):
                sql_3 = gr.Code(label="Final SQL (Bind-Resolved Preview)", language="sql")
                resolved_sql_3 = gr.Code(
                    label="Unused",
                    language="sql",
                    visible=False,
                )
                desc_3 = gr.Textbox(label="Explanation", lines=6)
                rationale_3 = gr.Textbox(label="Rationale", lines=2)
                risk_3 = gr.Textbox(label="Risk notes", lines=2)

            option_selector = gr.Radio(
                label="Auto-selected option (optional manual override)",
                choices=[],
            )
            run_btn = gr.Button(
                "Run Selected SQL (Optional Manual Override)",
                variant="secondary",
            )

        with gr.Column(elem_id="panel"):
            run_status = gr.Markdown(label="Execution status")
            result_table = gr.Dataframe(label="Result preview", wrap=True)
            summary_box = gr.Textbox(label="Human-readable summary", lines=10)
            chart_plan_box = gr.Textbox(
                label="Chart Plan (Deaktif)",
                lines=12,
                interactive=False,
            )
            result_file = gr.File(label="Download Excel")

        generate_btn.click(
            fn=generate_sql_options,
            inputs=[
                request_box,
                agent_selector,
                jdbc_url_box,
                oracle_user_box,
                oracle_password_box,
                all_metadata_checkbox,
                parallel_selector,
            ],
            outputs=[
                generation_status,
                sql_1,
                resolved_sql_1,
                desc_1,
                rationale_1,
                risk_1,
                sql_2,
                resolved_sql_2,
                desc_2,
                rationale_2,
                risk_2,
                sql_3,
                resolved_sql_3,
                desc_3,
                rationale_3,
                risk_3,
                option_selector,
                context_state,
                run_status,
                result_table,
                summary_box,
                chart_plan_box,
                result_file,
                suggestion_box,
                suggestion_state,
            ],
        ).then(
            fn=_complete_deferred_summary,
            inputs=[context_state, result_table, summary_box, chart_plan_box],
            outputs=[summary_box, chart_plan_box],
        )

        def _apply_suggestion(suggested_text: str) -> str:
            return str(suggested_text).strip()

        resubmit_btn.click(
            fn=_apply_suggestion,
            inputs=[suggestion_state],
            outputs=[request_box],
        )

        run_btn.click(
            fn=run_selected_sql,
            inputs=[
                option_selector,
                context_state,
                jdbc_url_box,
                oracle_user_box,
                oracle_password_box,
                parallel_selector,
            ],
            outputs=[run_status, result_table, summary_box, chart_plan_box, result_file],
        )

    return demo


if __name__ == "__main__":
    app = build_app()
    configured_port = os.getenv("GRADIO_SERVER_PORT", "").strip()
    launch_kwargs: dict[str, Any] = {
        "server_name": "0.0.0.0",
        "css": APP_CSS,
    }
    if configured_port:
        launch_kwargs["server_port"] = int(configured_port)
    app.launch(**launch_kwargs)
