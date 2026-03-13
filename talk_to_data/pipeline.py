"""Orchestrates extraction, retrieval, SQL generation, execution, and artifacts."""

from __future__ import annotations

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
from .runs import create_run_dir, save_result_excel, save_result_preview, save_run_artifacts
from .sql_explainer import describe_sql_candidate
from .sql_generator import SQLGenerationError, generate_sql_candidates
from .sql_guardrails import SQLGuardrailError, validate_sql_before_execution
from .sql_judge import choose_best_sql_candidate
from .summarizer import summarize_result_to_text


class PipelineError(RuntimeError):
    """General pipeline user-facing error."""


@dataclass
class CandidateRunResult:
    dataframe: pd.DataFrame
    summary: str
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

        metadata_overview = build_metadata_overview(metadata_docs)

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

        try:
            candidates = generate_sql_candidates(
                user_request=request,
                requirements=requirements,
                metadata=metadata_used,
                llm_client=self.llm_client,
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

        run_dir = create_run_dir(self.config.runs_dir)
        save_run_artifacts(
            run_dir,
            user_request=request,
            requirements=requirements,
            metadata_used=metadata_used,
            sql_candidates=candidates,
            agent_info={
                "id": agent.id,
                "label": agent.label,
                "description": agent.description,
                "metadata_path": str(agent.metadata_path),
            },
            judge_result=judge_result,
        )

        return {
            "run_dir": str(run_dir),
            "request": request,
            "agent_id": agent.id,
            "agent_label": agent.label,
            "agent_metadata_path": str(agent.metadata_path),
            "requirements": requirements,
            "metadata_used": metadata_used,
            "candidates": candidates,
            "recommended_candidate_id": recommended_candidate_id,
            "selection_mode": str(judge_result.get("selection_mode", "fallback")),
            "judge_result": judge_result,
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

        try:
            validate_sql_before_execution(
                sql,
                metadata_used,
                llm_client=self.llm_client,
            )
        except SQLGuardrailError as exc:
            raise PipelineError(f"Execution blocked by SQL guardrails: {exc}") from exc

        active_config = self._apply_connection_override(connection)
        try:
            dataframe = execute_sql(sql, requirements, active_config)
        except DatabaseExecutionError as exc:
            hint = (
                "Hint: check bind variables (:report_period, :start_date, :end_date, :n), "
                "mandatory filters, and Oracle object names."
            )
            raise PipelineError(f"Oracle execution error: {exc}. {hint}") from exc

        run_dir_str = context.get("run_dir")
        run_dir = Path(run_dir_str) if run_dir_str else create_run_dir(self.config.runs_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        summary = summarize_result_to_text(
            dataframe,
            user_request=str(context.get("request", "")),
            sql=sql,
            llm_client=self.llm_client,
            llm_enabled=self.config.llm_summarizer_enabled,
        )
        excel_path = save_result_excel(dataframe, run_dir)
        save_result_preview(dataframe, run_dir)

        return CandidateRunResult(
            dataframe=dataframe,
            summary=summary,
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
