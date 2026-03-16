"""Orchestrates extraction, retrieval, SQL generation, execution, and artifacts."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from .agent_registry import (
    AgentConfig,
    AgentRegistry,
    AgentRegistryError,
    load_agent_registry,
)
from .config import AppConfig
from .db import DatabaseExecutionError, execute_sql
from .llm_client import LLMClient, try_build_llm_client
from .metadata_retriever import (
    MetadataFileError,
    build_metadata_overview,
    load_metadata_documents,
    retrieve_relevant_metadata,
)
from .requirements_extractor import RequirementsExtractionError, extract_requirements
from .runs import (
    create_run_dir,
    save_json_artifact,
    save_result_excel,
    save_result_interpretation,
    save_result_preview,
    save_run_artifacts,
)
from .sql_explainer import describe_sql_candidate
from .sql_generator import SQLGenerationError, generate_sql_candidates
from .sql_guardrails import SQLGuardrailError, validate_sql_before_execution
from .sql_judge import choose_best_sql_candidate
from .sql_validation import build_validation_catalog
from .summarizer import summarize_result


class PipelineError(RuntimeError):
    """General pipeline user-facing error."""


@dataclass
class CandidateRunResult:
    dataframe: pd.DataFrame
    summary: str
    chart_plan: dict[str, Any] | None
    summary_mode: str
    fallback_reason: str | None
    validation_errors: list[str]
    excel_path: Path


class TalkToDataService:
    """Main service class for app workflow."""

    def __init__(self, config: AppConfig | None = None, llm_client: LLMClient | None = None):
        self.config = config or AppConfig.from_env()
        self.llm_client = llm_client or try_build_llm_client(
            api_key=self.config.llm_api_key,
            url=self.config.llm_url,
            model=self.config.llm_model,
            timeout_sec=self.config.llm_timeout_sec,
        )
        self._metadata_documents_by_path: dict[Path, list[dict[str, Any]]] = {}
        self._agent_registry: AgentRegistry | None = None

    def list_agents(self) -> list[dict[str, str]]:
        """Return configured query agents in display order."""
        registry = self._load_agent_registry()
        return registry.list_agents()

    def prepare_candidates(
        self,
        user_request: str,
        agent_id: str | None = None,
    ) -> dict[str, Any]:
        """End-to-end generation before execution step."""
        request = user_request.strip()
        if not request:
            raise PipelineError("Please provide a request.")

        agent = self._resolve_agent(agent_id)

        try:
            metadata_docs = self._get_metadata_documents(agent.metadata_path)
        except MetadataFileError as exc:
            raise PipelineError(
                f"Selected agent metadata could not be loaded "
                f"(agent='{agent.id}', path='{agent.metadata_path}'): {exc}"
            ) from exc

        metadata_overview = build_metadata_overview(
            metadata_docs,
            metadata_path=agent.metadata_path,
        )
        validation_catalog = build_validation_catalog(metadata_docs)

        try:
            requirements = extract_requirements(
                request,
                llm_client=self.llm_client,
                metadata_overview=metadata_overview,
            )
        except RequirementsExtractionError as exc:
            raise PipelineError(f"Requirement extraction failed: {exc}") from exc

        if requirements.get("invalid_request"):
            reason = str(requirements.get("notes") or "Request marked as invalid by extractor.").strip()
            requirements["invalid_request"] = False
            existing_notes = str(requirements.get("notes", "")).strip()
            requirements["notes"] = (
                f"{existing_notes} Proceeding with conservative SQL generation."
                if existing_notes
                else f"{reason} Proceeding with conservative SQL generation."
            )

        metadata_used = retrieve_relevant_metadata(
            requirements=requirements,
            user_request=request,
            documents=metadata_docs,
            metadata_path=agent.metadata_path,
            top_k=200,
        )
        run_dir = create_run_dir(self.config.runs_dir)
        attempt_one_candidates: list[dict[str, Any]] | None = None
        attempt_one_judge_result: dict[str, Any] | None = None
        retry_decision: dict[str, Any] | None = None

        final_candidates: list[dict[str, Any]] | None = None
        final_judge_result: dict[str, Any] | None = None
        final_attempt = 0
        retry_context: dict[str, Any] | None = None

        for attempt in (1, 2):
            try:
                candidates = generate_sql_candidates(
                    user_request=request,
                    requirements=requirements,
                    metadata=metadata_used,
                    llm_client=self.llm_client,
                    retry_context=retry_context,
                )
            except SQLGenerationError as exc:
                raise PipelineError(f"SQL generation failed: {exc}") from exc

            for candidate in candidates:
                candidate["description"] = describe_sql_candidate(
                    candidate,
                    metadata_used,
                    llm_client=self.llm_client,
                )

            judge_result = choose_best_sql_candidate(
                user_request=request,
                metadata_used=metadata_used,
                candidates=candidates,
                llm_client=self.llm_client,
                validation_catalog=validation_catalog,
            )
            recommended_candidate_id = str(
                judge_result.get("recommended_candidate_id", "")
            ).strip()
            if not recommended_candidate_id and candidates:
                recommended_candidate_id = str(candidates[0].get("id", "option_1")).strip()
            for candidate in candidates:
                candidate["recommended"] = (
                    str(candidate.get("id", "")).strip() == recommended_candidate_id
                )

            retry_recommended = bool(judge_result.get("retry_recommended", False))
            if attempt == 1 and retry_recommended:
                attempt_one_candidates = deepcopy(candidates)
                attempt_one_judge_result = deepcopy(judge_result)
                retry_context = _build_retry_context(judge_result, candidates)
                retry_decision = {
                    "retry_triggered": True,
                    "trigger_attempt": 1,
                    "trigger_reason": _retry_reason(judge_result),
                    "judge_error_kind": str(judge_result.get("judge_error_kind", "none")),
                    "all_candidates_disqualified": bool(
                        judge_result.get("all_candidates_disqualified", False)
                    ),
                    "disqualified_count": int(judge_result.get("disqualified_count", 0)),
                }
                continue

            final_candidates = candidates
            final_judge_result = judge_result
            final_attempt = attempt
            break

        if final_candidates is None or final_judge_result is None:
            raise PipelineError("SQL candidate generation did not produce a final result.")

        recommended_candidate_id = str(
            final_judge_result.get("recommended_candidate_id", "")
        ).strip()
        if not recommended_candidate_id and final_candidates:
            recommended_candidate_id = str(final_candidates[0].get("id", "option_1")).strip()
        for candidate in final_candidates:
            candidate["recommended"] = (
                str(candidate.get("id", "")).strip() == recommended_candidate_id
            )

        if (
            final_attempt == 2
            and bool(final_judge_result.get("all_candidates_disqualified", False))
        ):
            save_run_artifacts(
                run_dir,
                user_request=request,
                requirements=requirements,
                metadata_used=metadata_used,
                sql_candidates=final_candidates,
                agent_info={
                    "id": agent.id,
                    "label": agent.label,
                    "description": agent.description,
                    "metadata_path": str(agent.metadata_path),
                },
                judge_result=final_judge_result,
            )
            if attempt_one_candidates is not None and attempt_one_judge_result is not None:
                save_json_artifact(run_dir, "sql_candidates_attempt_1.json", attempt_one_candidates)
                save_json_artifact(run_dir, "judge_result_attempt_1.json", attempt_one_judge_result)
            if retry_decision is not None:
                save_json_artifact(run_dir, "retry_decision.json", retry_decision)
            raise PipelineError(
                "Auto-selection failed after one retry: all SQL candidates were disqualified."
            )

        save_run_artifacts(
            run_dir,
            user_request=request,
            requirements=requirements,
            metadata_used=metadata_used,
            sql_candidates=final_candidates,
            agent_info={
                "id": agent.id,
                "label": agent.label,
                "description": agent.description,
                "metadata_path": str(agent.metadata_path),
            },
            judge_result=final_judge_result,
        )
        if attempt_one_candidates is not None and attempt_one_judge_result is not None:
            save_json_artifact(run_dir, "sql_candidates_attempt_1.json", attempt_one_candidates)
            save_json_artifact(run_dir, "judge_result_attempt_1.json", attempt_one_judge_result)
        if retry_decision is not None:
            save_json_artifact(run_dir, "retry_decision.json", retry_decision)

        return {
            "run_dir": str(run_dir),
            "request": request,
            "agent_id": agent.id,
            "agent_label": agent.label,
            "agent_metadata_path": str(agent.metadata_path),
            "requirements": requirements,
            "metadata_used": metadata_used,
            "validation_catalog": validation_catalog,
            "candidates": final_candidates,
            "recommended_candidate_id": recommended_candidate_id,
            "selection_mode": str(final_judge_result.get("selection_mode", "fallback")),
            "judge_result": final_judge_result,
            "attempt_count": final_attempt,
            "retry_attempted": final_attempt > 1,
            "llm_mode": "enabled" if self.llm_client is not None else "heuristic_fallback",
        }

    def execute_selected_candidate(
        self,
        context: dict[str, Any],
        candidate_id: str,
        connection: dict[str, str] | None = None,
    ) -> CandidateRunResult:
        """Execute selected SQL candidate and persist output artifact."""
        if not context:
            raise PipelineError("No generation context found. Generate SQL options first.")

        candidates = context.get("candidates")
        if not isinstance(candidates, list):
            raise PipelineError("SQL candidates are missing in context.")

        selected_id = str(candidate_id or "").strip()
        if not selected_id:
            selected_id = str(context.get("recommended_candidate_id", "")).strip()
        if not selected_id:
            raise PipelineError("No SQL option id provided.")

        selected = None
        for candidate in candidates:
            if isinstance(candidate, dict) and str(candidate.get("id")) == selected_id:
                selected = candidate
                break
        if selected is None:
            raise PipelineError(f"Selected option '{selected_id}' was not found.")

        sql = str(selected.get("sql", "")).strip()
        if not sql:
            raise PipelineError("Selected SQL is empty.")

        requirements = context.get("requirements")
        if not isinstance(requirements, dict):
            raise PipelineError("Requirements are missing in context.")
        metadata_used = context.get("metadata_used")
        if not isinstance(metadata_used, dict):
            raise PipelineError("Metadata context is missing.")
        validation_catalog = context.get("validation_catalog")
        if not isinstance(validation_catalog, dict):
            validation_catalog = None

        try:
            validate_sql_before_execution(
                sql,
                metadata_used,
                llm_client=self.llm_client,
                validation_catalog=validation_catalog,
            )
        except SQLGuardrailError as exc:
            raise PipelineError(f"Execution blocked by SQL guardrails: {exc}") from exc

        active_config = self._apply_connection_override(connection)
        try:
            dataframe = execute_sql(sql, requirements, active_config)
        except DatabaseExecutionError as exc:
            hint = (
                "Hint: check bind variables (:report_period, :year_value, :date_value, :start_date, :end_date, :n), "
                "mandatory filters, and Oracle object names."
            )
            raise PipelineError(f"Oracle execution error: {exc}. {hint}") from exc

        run_dir_str = context.get("run_dir")
        run_dir = Path(run_dir_str) if run_dir_str else create_run_dir(self.config.runs_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        interpreted = summarize_result(
            dataframe,
            user_request=str(context.get("request", "")),
            sql=sql,
            metadata_used=metadata_used,
            llm_client=self.llm_client,
            llm_enabled=self.config.llm_summarizer_enabled,
            chart_render_enabled=self.config.result_chart_render_enabled,
        )
        if self.config.llm_summarizer_required and interpreted.summary_mode != "llm":
            reason = interpreted.fallback_reason or "LLM summary was not produced."
            raise PipelineError(
                "LLM summary is required but unavailable. "
                f"Reason: {reason}"
            )

        summary = interpreted.summary_text
        chart_plan = interpreted.chart_plan
        excel_path = save_result_excel(dataframe, run_dir)
        save_result_preview(dataframe, run_dir)
        save_result_interpretation(
            run_dir,
            {
                "summary": summary,
                "chart_plan": chart_plan,
                "llm_used": interpreted.llm_used,
                "summary_mode": interpreted.summary_mode,
                "fallback_reason": interpreted.fallback_reason,
                "validation_errors": interpreted.validation_errors,
                "chart_render_enabled": interpreted.chart_render_enabled,
                "selected_candidate_id": selected_id,
                "executed_sql": sql,
            },
        )

        return CandidateRunResult(
            dataframe=dataframe,
            summary=summary,
            chart_plan=chart_plan,
            summary_mode=interpreted.summary_mode,
            fallback_reason=interpreted.fallback_reason,
            validation_errors=interpreted.validation_errors,
            excel_path=excel_path,
        )

    def _resolve_agent(self, agent_id: str | None) -> AgentConfig:
        registry = self._load_agent_registry()
        try:
            return registry.resolve(agent_id)
        except AgentRegistryError as exc:
            raise PipelineError(str(exc)) from exc

    def _load_agent_registry(self) -> AgentRegistry:
        if self._agent_registry is not None:
            return self._agent_registry
        try:
            self._agent_registry = load_agent_registry(self.config.agent_registry_path)
        except AgentRegistryError as exc:
            raise PipelineError(str(exc)) from exc
        return self._agent_registry

    def _get_metadata_documents(self, metadata_path: Path) -> list[dict[str, Any]]:
        key = metadata_path.resolve()
        cached = self._metadata_documents_by_path.get(key)
        if cached is not None:
            return cached

        documents = load_metadata_documents(metadata_path)
        self._metadata_documents_by_path[key] = documents
        return documents

    def _apply_connection_override(
        self,
        connection: dict[str, str] | None,
    ) -> AppConfig:
        if not connection:
            return self.config

        user = str(connection.get("user") or self.config.oracle_user).strip()
        password = str(connection.get("password") or self.config.oracle_password)
        dsn_input = str(connection.get("dsn") or self.config.oracle_dsn).strip()
        dsn = _normalize_oracle_dsn(dsn_input)

        return replace(
            self.config,
            oracle_user=user,
            oracle_password=password,
            oracle_dsn=dsn,
        )


def _normalize_oracle_dsn(dsn: str) -> str:
    value = dsn.strip()
    if not value:
        return value

    lower = value.lower()
    prefix = "jdbc:oracle:thin:@"
    if lower.startswith(prefix):
        return value[len(prefix) :].strip()
    return value


def _retry_reason(judge_result: dict[str, Any]) -> str:
    if bool(judge_result.get("all_candidates_disqualified", False)):
        return "all_candidates_disqualified"
    kind = str(judge_result.get("judge_error_kind", "")).strip().lower()
    if kind in {"llm_error", "parse_error"}:
        return kind
    return "unspecified"


def _build_retry_context(
    judge_result: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    disqualify_reasons: list[str] = []
    blocked_sql_patterns: list[str] = []

    candidate_evaluations = judge_result.get("candidate_evaluations")
    if isinstance(candidate_evaluations, list):
        for item in candidate_evaluations:
            if not isinstance(item, dict):
                continue
            for reason in item.get("disqualify_reasons", []):
                text = str(reason).strip()
                if text and text not in disqualify_reasons:
                    disqualify_reasons.append(text)

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        sql = str(candidate.get("sql", "")).strip()
        if not sql:
            continue
        sql_low = sql.lower()
        patterns = (
            "select *",
            ":report_period",
            ":year_value",
            ":date_value",
            "report_period",
            "tanzim_tarih_id",
            "tarih_id",
        )
        for pattern in patterns:
            if pattern in sql_low and pattern not in blocked_sql_patterns:
                blocked_sql_patterns.append(pattern)

    return {
        "disqualify_reasons": disqualify_reasons[:20],
        "blocked_sql_patterns": blocked_sql_patterns[:12],
    }
