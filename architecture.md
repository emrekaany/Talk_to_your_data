# Architecture

This document is the canonical architecture reference for this repository.
All agents must read and understand this file before making code changes.

## System Purpose

The system converts a natural-language analytics request into safe Oracle SQL options, lets the user choose one option, executes it, and returns:

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
3. UI renders 3 options and a selector.
4. User selects one option and clicks run.
5. `TalkToDataService.execute_selected_candidate()`:
   - validates SQL guardrails before execution
   - executes SQL on Oracle using bind params
   - summarizes result (LLM optional, heuristic fallback)
   - saves execution artifacts (`result.xlsx`, `result_preview.csv`)
6. UI shows run status, preview dataframe, summary text, and downloadable Excel.

## Component Responsibilities

- `app.py`
  - Gradio UI and callbacks (`generate_sql_options`, `run_selected_sql`).
  - Holds generation context in UI state.
- `talk_to_data/pipeline.py`
  - Main orchestrator (`TalkToDataService`).
  - Coordinates extract -> retrieve -> generate -> explain -> execute.
- `talk_to_data/agent_registry.py`
  - Loads and validates `metadata/agents/agents.json`.
  - Resolves selected/default agent and metadata path.
- `talk_to_data/config.py`
  - Environment-driven runtime config.
  - Loads root `.env` (dotenv if available, fallback parser otherwise).
- `talk_to_data/requirements_extractor.py`
  - Produces normalized structured request requirements.
  - Uses LLM path with retry/fix-json and heuristic fallback.
- `talk_to_data/metadata_retriever.py`
  - Loads metadata JSON documents.
  - Performs high-recall token/cosine retrieval (up to top 200).
  - Produces compact relevant metadata + mandatory rules + guardrails.
- `talk_to_data/sql_generator.py`
  - Requires LLM and generates exactly 3 candidates.
  - Enforces SQL safety constraints, mandatory filters, and row limit.
  - Repairs malformed/unsafe candidates via LLM normalization/repair path.
- `talk_to_data/sql_explainer.py`
  - Generates plain-language explanation per SQL candidate.
- `talk_to_data/sql_guardrails.py`
  - Execution-time safety + allowlist + alias/column metadata + mandatory obligation validation.
- `talk_to_data/sql_validation.py`
  - Shared alias/column metadata validation helper used by guardrails and SQL judge.
- `talk_to_data/db.py`
  - Oracle driver access and SQL execution with bind mapping.
  - Sanitizes error output to avoid secret leakage.
- `talk_to_data/summarizer.py`
  - Result summarization (heuristic default, optional LLM mode).
- `talk_to_data/runs.py`
  - Timestamped run directory creation and artifact persistence.
- `talk_to_data/llm_client.py`
  - OpenAI-compatible chat wrapper.
- `talk_to_data/llm_logging.py`
  - JSONL prompt logging for all outbound prompts.

## Core Contracts

- `extract_requirements(user_request, llm_client, metadata_overview) -> dict`
- `retrieve_relevant_metadata(requirements, user_request, documents, metadata_path, top_k) -> dict`
- `generate_sql_candidates(user_request, requirements, metadata, llm_client) -> list[dict]`
- `describe_sql_candidate(candidate, metadata, llm_client) -> str`
- `summarize_result_to_text(df, user_request, sql, llm_client, llm_enabled) -> str`
- `TalkToDataService.prepare_candidates(user_request, agent_id) -> dict`
- `TalkToDataService.execute_selected_candidate(context, candidate_id, connection) -> CandidateRunResult`

## Safety and Guardrails

- Only `SELECT`/`WITH` queries are allowed.
- Blocked operations/functions include DML/DDL and unsafe database packages.
- Multiple statements are blocked.
- `SELECT *` is blocked.
- SQL comments are blocked.
- Mandatory filters from requirements/metadata are enforced.
- Oracle row limit is enforced: `FETCH FIRST 200 ROWS ONLY`.
- Execution checks include:
  - safety validation
  - table allowlist validation
  - alias.column vs metadata table-column validation
  - mandatory filter obligation validation
- Bind placeholders are resolved from normalized requirements.

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
- `agent_info.json` (agent-based flow)

Execution artifacts:

- `result_preview.csv`
- `result.xlsx`

Global LLM prompt log:

- `runs/llm_prompts.log`

## Configuration Boundaries

Primary env vars:

- LLM: `LLM_API_KEY` (or `OPENAI_API_KEY`), `LLM_URL`, `LLM_MODEL`, `LLM_TIMEOUT_SEC`, `LLM_SUMMARIZER_ENABLED`, `LLM_PROMPT_LOG_PATH`
- Oracle: `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`
- Paths: `METADATA_VECTORED_PATH`, `AGENT_REGISTRY_PATH`, `RUNS_DIR`

Secrets must remain env-driven and must not be hardcoded.

## Invariants (Do Not Break)

1. End-to-end flow stays: request -> extraction -> metadata retrieval -> 3 SQL options -> selection -> Oracle execution -> preview/summary/excel.
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
