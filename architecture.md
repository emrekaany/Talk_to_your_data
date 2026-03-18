# Architecture

This document is the canonical architecture reference for this repository.
All agents must read and understand this file before making code changes.
When coding with Codex, this file is the primary source of architecture truth.

## System Purpose

The system converts a natural-language analytics request into safe Oracle SQL options, auto-selects the best option (with manual override support), executes it, and returns:

- preview table
- human-readable summary
- Excel export

## End-to-End Flow

1. User selects an agent and enters a request in the Gradio UI (`app.py`).
2. `TalkToDataService.prepare_candidates()` orchestrates generation:
   - resolve agent from registry
   - load agent-specific metadata documents
   - extract structured requirements
   - retrieve relevant metadata context
   - generate exactly 3 SQL candidates
   - explain each candidate
   - persist generation artifacts under `runs/<timestamp>/`
3. UI renders 3 options and marks the recommended option selected by SQL judge (`LLM + fallback`).
4. `generate_sql_options()` auto-runs the recommended candidate via `TalkToDataService.execute_selected_candidate()`.
5. User can optionally override selection and rerun using `Run Selected SQL`.
6. `TalkToDataService.execute_selected_candidate()`:
   - validates SQL guardrails before execution
   - executes SQL on Oracle using bind params
   - summarizes result with `summary_mode` visibility (`llm` or `heuristic`)
   - validates chart plan payload against result dataframe
   - keeps chart rendering deactivated unless explicitly enabled
   - saves execution artifacts (`result.xlsx`, `result_preview.csv`)
7. UI shows run status, preview dataframe, summary text, chart plan text (deactivated), and downloadable Excel.

## Component Responsibilities

- `app.py`
  - Gradio UI and callbacks (`generate_sql_options`, `run_selected_sql`).
  - Holds generation context in UI state.
  - Auto-executes recommended SQL after generation; manual rerun remains available.
- `talk_to_data/pipeline.py`
  - Main orchestrator (`TalkToDataService`).
  - Coordinates extract -> retrieve -> generate -> explain -> execute.
  - Runs generate+judge flow with max two attempts.
  - Triggers second attempt when judge fails or all candidates are disqualified.
  - Fail-fast blocks execution when second attempt still has all candidates disqualified.
  - Persists retry attempt artifacts for observability.
- `talk_to_data/agent_registry.py`
  - Loads and validates `metadata/agents/agents.json`.
  - Resolves selected/default agent and metadata path.
- `talk_to_data/config.py`
  - Environment-driven runtime config.
  - Loads root `.env` (dotenv if available, fallback parser otherwise).
- `talk_to_data/requirements_extractor.py`
  - Produces normalized structured request requirements.
  - Sends the original user request to LLM as-is (no request-side prompt transformation).
  - Uses single-pass strict JSON parsing (no fix-json retry path).
  - Applies only minimal type normalization and heuristic fallback when LLM is unavailable.
- `talk_to_data/metadata_retriever.py`
  - Loads metadata JSON documents.
  - Enforces JSON and document-shape validity at metadata load time.
  - Join-key column quality gate is currently disabled at load time (documented in change log).
  - Performs high-recall token/cosine retrieval (up to top 2000).
  - Produces compact relevant metadata + static `mandatory_rules` + runtime `runtime_mandatory_rules` container + guardrails.
- `talk_to_data/sql_generator.py`
  - Requires LLM and generates exactly 3 candidates.
  - Accepts optional `retry_context` so second-attempt generation can avoid first-attempt failure patterns.
  - Uses prompt engineering (at SQL generation stage) for temporal token interpretation (`YYYY`, `YYYYMM`, `YYYYMMDD`).
  - Parses strict JSON candidate output and validates safety + mandatory filters.
  - Does not rewrite/repair/normalize LLM SQL text; invalid candidates are rejected.
  - Enforces row-limit policy via validation (`FETCH FIRST 200 ROWS ONLY`) and blocks semicolon-delimited SQL.
- `talk_to_data/sql_explainer.py`
  - Generates plain-language explanation per SQL candidate.
- `talk_to_data/sql_guardrails.py`
  - Execution-time safety + allowlist + alias/column metadata + mandatory obligation validation.
  - Evaluates both static `mandatory_rules` and runtime `runtime_mandatory_rules`.
  - Does not synthesize new obligations from performance-rule text.
  - Accepts optional full `validation_catalog` for execution-time column checks.
- `talk_to_data/sql_validation.py`
  - Builds full validation catalog from raw metadata documents.
  - Supports quoted/unquoted alias/column parsing and schema-qualified reference handling.
  - Detects ambiguous bare-table mappings and unknown alias.column references.
- `talk_to_data/sql_judge.py`
  - Evaluates candidates with LLM judge + deterministic fallback.
  - Emits `judge_error_kind`, `all_candidates_disqualified`, `disqualified_count`, and `retry_recommended` for orchestration.
- `talk_to_data/db.py`
  - Oracle driver access and SQL execution with bind mapping.
  - Resolves generic placeholder binds from requirements (legacy support for `:report_period`, `:year_value`, `:date_value` remains for compatibility).
  - Sanitizes error output to avoid secret leakage.
- `talk_to_data/summarizer.py`
  - Result summarization (heuristic default, optional LLM mode).
  - Emits `ResultInterpretation` with `summary_mode`, `fallback_reason`, and `validation_errors`.
  - Contains strict `validate_chart_plan(plan, df)` entry point.
- `talk_to_data/runs.py`
  - Timestamped run directory creation and artifact persistence.
- `talk_to_data/llm_client.py`
  - OpenAI-compatible chat wrapper.
- `talk_to_data/llm_logging.py`
  - JSONL prompt logging for all outbound prompts.

## Core Contracts

- `extract_requirements(user_request, llm_client, metadata_overview) -> dict`
- `retrieve_relevant_metadata(requirements, user_request, documents, metadata_path, top_k) -> dict`
- `generate_sql_candidates(user_request, requirements, metadata, llm_client, retry_context=None) -> list[dict]`
- `describe_sql_candidate(candidate, metadata, llm_client) -> str`
- `choose_best_sql_candidate(user_request, metadata_used, candidates, llm_client, validation_catalog) -> dict`
- `validate_sql_before_execution(sql, metadata_used, llm_client, validation_catalog) -> None`
- `summarize_result_to_text(df, user_request, sql, metadata_used, llm_client, llm_enabled, chart_render_enabled) -> str`
- `summarize_result(df, user_request, sql, metadata_used, llm_client, llm_enabled, chart_render_enabled) -> ResultInterpretation`
- `TalkToDataService.prepare_candidates(user_request, agent_id) -> dict`
- `TalkToDataService.execute_selected_candidate(context, candidate_id, connection) -> CandidateRunResult`

## Safety and Guardrails

- Only `SELECT`/`WITH` queries are allowed.
- Blocked operations/functions include DML/DDL and unsafe database packages.
- Multiple statements are blocked.
- `SELECT *` is blocked.
- SQL comments are blocked.
- Mandatory filters from requirements/metadata are enforced.
- Metadata load enforces JSON/document-shape validity before SQL generation.
- SQL generation does not mutate LLM SQL text; invalid candidates fail fast.
- Missing mandatory filters are blocked via validation (no SQL text injection/repair).
- Oracle row limit is enforced: `FETCH FIRST 200 ROWS ONLY`.
- Execution checks include:
  - safety validation
  - table allowlist validation
  - alias.column vs metadata table-column validation
  - ambiguous bare-table reference validation across schemas
  - mandatory filter obligation validation
- Full metadata `validation_catalog` can be supplied so execution-time checks are not limited by compact retrieval column caps.
- Bind placeholders are resolved from normalized requirements and generic requirement keys (date-range binds, row-limit aliases, and backward-compatible legacy binds).

## Multi-Agent Architecture

- Registry file: `metadata/agents/agents.json`
- Each agent has:
  - `id`
  - `label`
  - `metadata_path`
  - optional `description`
- Selected agent metadata file is the source of truth for retrieval and SQL generation context.
- Effective metadata source is recorded in `metadata_used.json` (`retrieval_debug.metadata_source`).

## Run Artifacts

Generation artifacts (`runs/<timestamp>/`):

- `request.txt`
- `requirements.json`
- `metadata_used.json`
- `sql_candidates.json`
- `judge_result.json`
- `sql_candidates_attempt_1.json` (retry path only)
- `judge_result_attempt_1.json` (retry path only)
- `retry_decision.json` (retry path only)
- `agent_info.json` (agent-based flow)

Execution artifacts:

- `result_preview.csv`
- `result.xlsx`
- `result_interpretation.json`

Global LLM prompt log:

- `runs/llm_prompts.log`

## Configuration Boundaries

Primary env vars:

- LLM: `LLM_API_KEY` (or `OPENAI_API_KEY`), `LLM_URL`, `LLM_MODEL`, `LLM_TIMEOUT_SEC`, `LLM_SUMMARIZER_ENABLED`, `LLM_SUMMARIZER_REQUIRED`, `LLM_PROMPT_LOG_PATH`
- Chart path: `RESULT_CHART_RENDER_ENABLED` (default disabled)
- Oracle: `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`
- Paths: `METADATA_VECTORED_PATH`, `AGENT_REGISTRY_PATH`, `RUNS_DIR`

Secrets must remain env-driven and must not be hardcoded.

## Invariants (Do Not Break)

1. End-to-end flow stays: request -> extraction -> metadata retrieval -> 3 SQL options -> best-option selection (LLM judge + fallback) -> Oracle execution -> preview/summary/excel.
2. Candidate generation must return exactly 3 SQL options.
3. SQL safety/guardrail checks must remain active before execution.
4. Run artifacts must persist under `runs/<timestamp>/`.
5. Core contracts and env-based secret handling must stay backward-compatible unless explicitly documented as breaking.

## Architecture Change Log (Mandatory)

Any architecture-impacting change must be recorded here by the implementing agent.

Entry format:

- `YYYY-MM-DD - AgentName: <what changed> | <why> | <affected modules/files>`

Entries:

- 2026-03-14 - Codex: Created canonical `architecture.md` from current implementation baseline, including invariants and mandatory architecture logging rule | Establish a single architecture source for all future agents | `architecture.md`
- 2026-03-14 - Codex: Added shared alias/column metadata validation and enforced it in execution-time guardrails; SQL judge now reuses the same validator | Remove duplicated logic and block unknown alias.column references before Oracle execution | `talk_to_data/sql_validation.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/sql_judge.py`, `architecture.md`
- 2026-03-15 - Codex: Added summary-mode observability (`llm`/`heuristic`), strict chart-plan validation, UI chart-plan visibility (deactivated), and strict LLM-required gate | Address fallback transparency, chart-plan correctness, and unused chart-plan visibility without enabling chart rendering | `talk_to_data/summarizer.py`, `talk_to_data/pipeline.py`, `app.py`, `architecture.md`
- 2026-03-15 - Codex: Added request-level time granularity extraction (`year|month|day`), SQL granularity-aware TO_CHAR mandatory filters, tanzim-path prioritization, and bind/guardrail support for `:year_value` + `:date_value` | Ensure temporal filters are explicit, bind-safe, and validated end-to-end | `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/db.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/pipeline.py`, `architecture.md`
- 2026-03-15 - Codex: Strengthened shared SQL validation with quoted identifier parsing, schema ambiguity blocking, and full-catalog execution checks; removed duplicate unknown-column reason path from judge | Close guardrail bypass/false-positive risks and keep judge disqualify reasons singular | `talk_to_data/sql_validation.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/sql_judge.py`, `talk_to_data/pipeline.py`, `architecture.md`
- 2026-03-16 - Codex: Reworked mandatory-filter handling to validation+repair (no SQL text injection), added controlled date-like NUMBER targeting for granular time filters, introduced `runtime_mandatory_rules`, expanded guardrail granular pattern validation, and added extractor hard-fail for invalid calendar date tokens | Close SQL breakage/false-negative risks while preserving 3-candidate flow and strict temporal safety | `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/sql_judge.py`, `talk_to_data/metadata_retriever.py`, `README.md`, `architecture.md`
- 2026-03-16 - Codex: Added metadata join-key quality gate, judge retry metadata contract, and one-shot generate+judge retry orchestration with fail-fast on second all-disqualified attempt; added retry artifacts and retry-aware SQL prompting | Improve auto-selection robustness under judge failures/disqualifications while keeping strict safety guarantees | `talk_to_data/metadata_retriever.py`, `talk_to_data/sql_judge.py`, `talk_to_data/sql_generator.py`, `talk_to_data/pipeline.py`, `talk_to_data/runs.py`, `README.md`, `architecture.md`
- 2026-03-17 - Codex: Disabled metadata join-key fail-fast at load time by removing runtime gate invocation so metadata files with partial join-key column coverage still load | Remove user-blocking metadata-load error while preserving rest of flow | `talk_to_data/metadata_retriever.py`, `README.md`, `architecture.md`
- 2026-03-17 - Codex: Synced architecture document to runtime behavior by documenting auto-run-on-generate UI flow, manual override path, and current metadata-load validation scope | Keep `architecture.md` accurate/canonical for all future Codex sessions | `architecture.md`
- 2026-03-17 - Codex: Removed request-side extraction transformations and SQL rewrite/repair paths; moved time-token handling to SQL-generation prompt rules; removed report-period-specific auto-obligation injection while preserving validation-first fail-fast behavior | Align runtime with raw-request prompting and no SQL mutation requirement while keeping safety invariants | `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/metadata_retriever.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/sql_judge.py`, `talk_to_data/pipeline.py`, `README.md`, `architecture.md`
