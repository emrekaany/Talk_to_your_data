"""Gradio app for Talk to Your Data."""

from __future__ import annotations

import os
import re
from typing import Any

import gradio as gr

from talk_to_data.pipeline import PipelineError, TalkToDataService


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


def _empty_candidate_outputs() -> tuple[str, ...]:
    return ("", "", "", "", "", "", "", "", "", "", "", "", "", "", "")


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


def _render_resolved_preview_sql(sql: str, requirements: dict[str, Any] | None) -> str:
    """Render SQL with bind placeholders replaced for display only."""
    if not sql:
        return ""
    if not isinstance(requirements, dict):
        return sql

    def repl(match: re.Match[str]) -> str:
        bind_name = match.group(1)
        value = _resolve_bind_value(bind_name, requirements)
        if value is None:
            return match.group(0)
        return _to_sql_literal(value)

    return re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", repl, sql)


def _resolve_bind_value(bind_name: str, requirements: dict[str, Any]) -> Any:
    lowered = bind_name.lower()
    if lowered == "report_period":
        return requirements.get("report_period")
    if lowered in ("start_date", "from_date", "date_start"):
        time_range = requirements.get("time_range")
        if isinstance(time_range, dict):
            return time_range.get("start")
        return None
    if lowered in ("end_date", "to_date", "date_end"):
        time_range = requirements.get("time_range")
        if isinstance(time_range, dict):
            return time_range.get("end")
        return None
    return requirements.get(bind_name)


def _to_sql_literal(value: Any) -> str:
    text = str(value).strip()
    if not text or text.lower() == "none":
        return "NULL"
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return text
    escaped = text.replace("'", "''")
    return f"'{escaped}'"


def generate_sql_options(
    user_request: str,
    agent_id: str | None = None,
    jdbc_url: str = "",
    username: str = "",
    password: str = "",
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
            None,
        )

    try:
        context = SERVICE.prepare_candidates(request, agent_id=agent_id)
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
            None,
        )

    candidates = context.get("candidates", [])
    if not isinstance(candidates, list) or len(candidates) != 3:
        c = _empty_candidate_outputs()
        return (
            "Generation failed: expected exactly 3 candidates.",
            *c,
            gr.update(choices=[], value=None),
            None,
            "",
            [],
            "",
            None,
        )

    requirements = context.get("requirements")
    if not isinstance(requirements, dict):
        requirements = {}

    radio_choices: list[tuple[str, str]] = []
    option_values: list[tuple[str, str, str, str, str]] = []
    for idx, candidate in enumerate(candidates, start=1):
        sql = str(candidate.get("sql", "")).strip()
        resolved_sql = _render_resolved_preview_sql(sql, requirements)
        desc = str(candidate.get("description", "")).strip()
        rationale = str(candidate.get("rationale_short", "")).strip()
        risk = str(candidate.get("risk_notes", "")).strip()
        candidate_id = str(candidate.get("id", f"option_{idx}"))
        option_values.append((sql, resolved_sql, desc, rationale, risk))
        radio_choices.append((f"{candidate_id}: {rationale or 'Candidate SQL'}", candidate_id))

    selected_agent_label = str(context.get("agent_label", "")).strip()
    selected_agent_id = str(context.get("agent_id", "")).strip()
    recommended_candidate_id = str(context.get("recommended_candidate_id", "")).strip()
    selection_mode = str(context.get("selection_mode", "fallback")).strip() or "fallback"
    if not recommended_candidate_id and radio_choices:
        recommended_candidate_id = radio_choices[0][1]
    status = (
        f"Generated 3 SQL options for agent '{selected_agent_label or selected_agent_id}'. "
        f"Auto-selected: {recommended_candidate_id or 'n/a'} ({selection_mode}). "
        f"Run folder: {context.get('run_dir')} "
        f"(LLM mode: {context.get('llm_mode')})."
    )
    selected_default = recommended_candidate_id or (radio_choices[0][1] if radio_choices else None)
    option_1 = option_values[0]
    option_2 = option_values[1]
    option_3 = option_values[2]

    run_status_text = ""
    result_preview: Any = []
    result_summary = ""
    result_file_path: str | None = None
    if selected_default:
        connection = {
            "dsn": jdbc_url.strip(),
            "user": username.strip(),
            "password": password,
        }
        try:
            auto_result = SERVICE.execute_selected_candidate(
                context,
                selected_default,
                connection=connection,
            )
            result_preview = auto_result.dataframe.head(500)
            result_summary = auto_result.summary
            result_file_path = str(auto_result.excel_path)
            run_status_text = (
                f"Auto-selected SQL '{selected_default}' executed successfully. "
                f"Rows: {len(auto_result.dataframe)}"
            )
        except PipelineError as exc:
            sql_text = _find_candidate_sql(context, selected_default)
            run_status_text = f"Auto-run failed for '{selected_default}': {exc}"
            if sql_text:
                run_status_text += f"\n\nSQL:\n{sql_text}"
    else:
        run_status_text = "Auto-run skipped: recommended SQL option was not resolved."

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
        result_file_path,
    )


def run_selected_sql(
    selected_option: str,
    context: dict[str, Any] | None,
    jdbc_url: str,
    username: str,
    password: str,
) -> tuple[str, Any, str, str | None]:
    if not context:
        return (
            "No SQL context found. First click 'Generate SQL Options'.",
            [],
            "",
            None,
        )

    selected = selected_option.strip()
    if not selected:
        selected = str(context.get("recommended_candidate_id", "")).strip()
    if not selected:
        return ("No SQL option resolved to run.", [], "", None)

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
        )
    except PipelineError as exc:
        sql_text = _find_candidate_sql(context, selected)
        error_message = f"Run failed: {exc}"
        if sql_text:
            error_message += f"\n\nSQL:\n{sql_text}"
        return (error_message, [], "", None)

    preview = result.dataframe.head(500)
    return (f"Query executed successfully. Rows: {len(result.dataframe)}", preview, result.summary, str(result.excel_path))


def _find_candidate_sql(context: dict[str, Any], candidate_id: str) -> str:
    candidates = context.get("candidates")
    if not isinstance(candidates, list):
        return ""
    for candidate in candidates:
        if isinstance(candidate, dict) and str(candidate.get("id")) == candidate_id:
            return str(candidate.get("sql", "")).strip()
    return ""


def build_app() -> gr.Blocks:
    with gr.Blocks(title="Talk to Your Data") as demo:
        context_state = gr.State(value=None)
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
            generate_btn = gr.Button("Generate SQL Options", variant="primary")
            generation_status = gr.Markdown(label="Generation status")

        with gr.Column(elem_id="panel"):
            gr.Markdown("### Did you mean this?")

            with gr.Accordion("Option 1", open=False):
                sql_1 = gr.Code(label="SQL", language="sql")
                resolved_sql_1 = gr.Code(
                    label="Resolved Preview SQL (display only)",
                    language="sql",
                )
                desc_1 = gr.Textbox(label="Explanation", lines=6)
                rationale_1 = gr.Textbox(label="Rationale", lines=2)
                risk_1 = gr.Textbox(label="Risk notes", lines=2)

            with gr.Accordion("Option 2", open=False):
                sql_2 = gr.Code(label="SQL", language="sql")
                resolved_sql_2 = gr.Code(
                    label="Resolved Preview SQL (display only)",
                    language="sql",
                )
                desc_2 = gr.Textbox(label="Explanation", lines=6)
                rationale_2 = gr.Textbox(label="Rationale", lines=2)
                risk_2 = gr.Textbox(label="Risk notes", lines=2)

            with gr.Accordion("Option 3", open=False):
                sql_3 = gr.Code(label="SQL", language="sql")
                resolved_sql_3 = gr.Code(
                    label="Resolved Preview SQL (display only)",
                    language="sql",
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
            result_file = gr.File(label="Download Excel")

        generate_btn.click(
            fn=generate_sql_options,
            inputs=[
                request_box,
                agent_selector,
                jdbc_url_box,
                oracle_user_box,
                oracle_password_box,
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
                result_file,
            ],
        )

        run_btn.click(
            fn=run_selected_sql,
            inputs=[
                option_selector,
                context_state,
                jdbc_url_box,
                oracle_user_box,
                oracle_password_box,
            ],
            outputs=[run_status, result_table, summary_box, result_file],
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
