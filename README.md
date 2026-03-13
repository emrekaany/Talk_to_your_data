# Talk to Your Data

Production-minded Gradio app that converts natural language requests into safe Oracle SQL options, lets the user choose one, executes it, and returns tabular + textual + Excel outputs.

## Scope

This repository implements the end-to-end workflow requested in `forhumans.md`:

1. Take user request from UI.
2. Extract structured requirements (Prompt Part 2.5 style).
3. Retrieve high-recall relevant metadata from selected agent metadata file.
4. Generate exactly 3 SQL candidates.
5. Save run artifacts into timestamped folders.
6. Explain each SQL in plain language.
7. Let user choose and execute one SQL on Oracle.
8. Summarize query results (optional LLM step with heuristic fallback).
9. Show result preview, summary text, and Excel download.

## Architecture Report

### High-Level Flow

1. `app.py` receives agent selection + natural-language request.
2. `TalkToDataService.prepare_candidates()` orchestrates:
   - `load_agent_registry()` + agent resolution
   - `extract_requirements()`
   - `retrieve_relevant_metadata()`
   - `generate_sql_candidates()`
   - `describe_sql_candidate()`
   - artifact persistence under `runs/<timestamp>/`
3. UI displays 3 candidate cards and a selector.
4. User picks candidate and clicks run.
5. `TalkToDataService.execute_selected_candidate()`:
   - executes SQL using Oracle driver and bind params
   - summarizes result (LLM optional via feature flag)
   - writes `result.xlsx`
6. UI returns status, preview table, summary text, and download file.

### Module Responsibilities

- `app.py`
  - Gradio layout and callbacks.
  - Generates options and runs selected SQL.
- `talk_to_data/config.py`
  - Loads env-based runtime configuration.
  - Auto-loads root `.env` with `python-dotenv` when available and built-in fallback parser otherwise.
- `talk_to_data/agent_registry.py`
  - Loads `metadata/agents/agents.json`.
  - Resolves selected/default agent and metadata path.
- `talk_to_data/llm_client.py`
  - OpenAI-compatible chat completion wrapper.
  - Centralized LLM error handling.
- `talk_to_data/llm_logging.py`
  - Persists each outbound LLM prompt as JSONL log entries.
- `talk_to_data/requirements_extractor.py`
  - Implements `extract_requirements(user_request: str) -> dict`.
  - Uses `talk_to_data/llm_client.py` for LLM calls.
  - JSON validation + one retry via fix-json prompt.
  - Heuristic fallback when LLM is unavailable.
- `talk_to_data/metadata_retriever.py`
  - Loads selected agent metadata JSON.
  - High-recall retrieval using Unicode-aware token cosine similarity.
  - Returns broader relevant payload (more tables/columns) and mandatory rules.
  - Supports high-recall retrieval up to top 200 metadata documents per request.
- `talk_to_data/sql_generator.py`
  - Implements `generate_sql_candidates(...) -> list[dict]`.
  - Produces exactly 3 candidates.
  - Uses `talk_to_data/llm_client.py` for LLM calls.
  - Builds LLM prompt with explicit `Metadata`, `Request`, and `Sql Rule` sections.
  - Includes column-level context in prompt (`type`, `description`, `semantic_type`, `keywords`, selected properties when available).
  - Carries metadata source trace (`retrieval_debug.metadata_source`) into prompt for auditability.
  - Requests SQL-only output first, then normalizes/parses into app candidate format.
  - Applies SQL safety checks and regeneration/repair path.
  - Fails with explicit error when model output cannot be normalized into 3 valid SQL candidates (no synthetic fallback SQL).
  - Normalizes mandatory filters (for example `REPORT_PERIOD` -> `REPORT_PERIOD = :report_period`) before enforcement.
  - Enforces Oracle row limit style `FETCH FIRST 200 ROWS ONLY`.
- `talk_to_data/sql_guardrails.py`
  - Execution-time SQL validation before Oracle run.
  - Re-checks safety, table allowlist, and mandatory filter obligations.
- `talk_to_data/sql_explainer.py`
  - Implements `describe_sql_candidate(...) -> str`.
- `talk_to_data/db.py`
  - Oracle execution and bind variable preparation.
  - Sanitized error messages (no secret leakage).
- `talk_to_data/summarizer.py`
  - Implements `summarize_result_to_text(df) -> str`.
  - Optional LLM summarizer path controlled by `LLM_SUMMARIZER_ENABLED`; heuristic fallback remains default.
- `talk_to_data/runs.py`
  - Run folder creation + JSON/text/Excel persistence.
- `talk_to_data/pipeline.py`
  - End-to-end application service orchestration.
  - Exposes `list_agents()` and agent-aware `prepare_candidates(...)`.

### Core Data Contracts

- `extract_requirements(user_request) -> dict`
  - includes: `intent`, `required_filters`, `measures`, `dimensions`, `grain`, `time_range`, `report_period`, `join_needs`, `row_limit`, `security_constraints`.
- `retrieve_relevant_metadata(requirements, user_request) -> dict`
  - includes: `relevant_items`, `guardrails`, `mandatory_rules`.
- `generate_sql_candidates(...) -> list[dict]`
  - each: `{id, sql, rationale_short, risk_notes}`.
- `describe_sql_candidate(candidate, metadata) -> str`
- `summarize_result_to_text(df) -> str`
  - optional args: `user_request`, `sql`, `llm_client`, `llm_enabled`
- `TalkToDataService.prepare_candidates(user_request, agent_id=None) -> dict`
- `TalkToDataService.list_agents() -> list[dict[str, str]]`

## Safety and Guardrails

- Only SELECT/CTE statements are allowed.
- Blocked keywords include: `DROP`, `DELETE`, `INSERT`, `UPDATE`, `MERGE`, `ALTER`, `TRUNCATE`.
- Multiple statements are blocked.
- `SELECT *` is blocked.
- Row limit is always enforced with `FETCH FIRST 200 ROWS ONLY`.
- Mandatory metadata filters are propagated and enforced.
- Bare mandatory filter names are normalized to bind-safe predicates before SQL generation.
- SQL output normalization rejects `INVALID_REQUEST` marker responses and keeps SQL-generation flow active.
- SQL generation fails fast when 3 valid candidates cannot be produced after normalization/repair.
- Bind placeholders are used for runtime values (`:report_period`, date binds, etc.).
- Oracle errors are sanitized to avoid leaking secrets.

## Run Artifacts

Each generation creates `runs/<timestamp>/` with:

- `requirements.json`
- `metadata_used.json`
- `sql_candidates.json`
- `request.txt`
- `agent_info.json` (when generation is agent-based)

Each execution also writes:

- `result.xlsx`
- `result_preview.csv`

Global LLM prompt log:

- `runs/llm_prompts.log` (JSONL; one entry per outbound prompt)

## Implemented in This Session

Added and wired the following:

- `app.py`
- `talk_to_data/__init__.py`
- `talk_to_data/config.py`
- `talk_to_data/llm_client.py`
- `talk_to_data/requirements_extractor.py`
- `talk_to_data/metadata_retriever.py`
- `talk_to_data/sql_generator.py`
- `talk_to_data/sql_explainer.py`
- `talk_to_data/sql_guardrails.py`
- `talk_to_data/db.py`
- `talk_to_data/summarizer.py`
- `talk_to_data/runs.py`
- `talk_to_data/pipeline.py`
- `requirements.txt`
- `.gitignore`
- this `README.md`

Validation executed:

- `py -m compileall app.py talk_to_data` passed.
- Runtime imports requiring external packages were not runnable until dependencies are installed locally.

## Local Setup

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

Run:

```powershell
py app.py
```

Open:

- `http://localhost:7860`

## Environment Variables

LLM:

- `LLM_API_KEY` (required for LLM mode)
- `OPENAI_API_KEY` (accepted alias for local `.env` usage)
- `LLM_URL` (optional; default provided in code)
- `LLM_MODEL` (optional; default `florence_v2`)
- `LLM_TIMEOUT_SEC` (optional; default `60`)
- `LLM_SUMMARIZER_ENABLED` (optional; `true/1/on` enables LLM result summarization step)
- `LLM_PROMPT_LOG_PATH` (optional; default `runs/llm_prompts.log`)
- Runtime LLM calls flow through `talk_to_data/llm_client.py`.
- `scripts/llm_prompt.py` remains a standalone CLI helper and auto-loads root `.env`; `talk_to_data/config.py` also auto-loads `.env` (with built-in fallback parser when `python-dotenv` is unavailable).

Local secret file:

- Create `.env` in project root for local development (recommended keys shown below).
- Replace placeholder tokens such as `sk-xxxx` with the real key value.
- Prefer this shape:
  - `LLM_API_KEY=sk-<real-token>`
  - `LLM_URL=http://query-insight-llm-router-ai.apps.ocpai.anadolusigorta.com.tr/v1/chat/completions`
  - `LLM_MODEL=florence_v2`
- `.env` is gitignored.

Oracle:

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`

UI defaults for Oracle connection panel:

- JDBC URL Template: `jdbc:oracle:thin:@//10.1.24.184:80/ODSPROD`
- Username: `oeus_ky4642`
- Password: defaults to `ORACLE_PASSWORD` env var when present

Paths:

- `METADATA_VECTORED_PATH` (default `metadata_vectored.json`)
- `AGENT_REGISTRY_PATH` (default `metadata/agents/agents.json`)
- `RUNS_DIR` (default `runs`)

## Agent Registry

Registry file:

- `metadata/agents/agents.json`

Default registry contract:

```json
{
  "default_agent_id": "hasar",
  "agents": [
    {
      "id": "hasar",
      "label": "Hasar",
      "metadata_path": "metadata_vectored_hasar.json",
      "description": "Hasar verisi agenti"
    },
    {
      "id": "uretim",
      "label": "Uretim",
      "metadata_path": "metadata_vectored_uretim.json",
      "description": "Uretim verisi agenti"
    },
    {
      "id": "satis",
      "label": "Satis",
      "metadata_path": "metadata_vectored_satis.json",
      "description": "Satis verisi agenti"
    }
  ]
}
```

## Agent Metadata Layout

Default files:

- `metadata/agents/metadata_vectored_hasar.json`
- `metadata/agents/metadata_vectored_uretim.json`
- `metadata/agents/metadata_vectored_satis.json`

Current bootstrap content for each file:

```json
{
  "documents": []
}
```

These are intentionally empty stubs. Until they are populated with valid metadata documents, generation is blocked for that agent with a clear error.

How to add a new agent:

1. Create metadata file under `metadata/agents/`.
2. Add a new entry in `metadata/agents/agents.json`.
3. Set `default_agent_id` if needed.
4. Restart the app.

## Metadata Source

Primary retrieval file:

- selected agent metadata file from registry (for example `metadata/agents/metadata_vectored_hasar.json`)
- Effective source path is stored at runtime in `metadata_used.json` under `retrieval_debug.metadata_source`.

If missing, app raises a clear error and writes:

- `metadata_vectored.schema.stub.json`

Stub explains expected schema for table docs, columns, joins, mandatory filters, and security notes.

## UI Flow

1. Select agent from `Agent` dropdown.
2. Enter request in textbox.
3. Open `Oracle Connection` and verify JDBC URL / username / password.
4. Click `Generate SQL Options`.
5. Review 3 SQL options with explanation and risk notes.
6. Select one option via radio.
7. Click `Run Selected SQL`.
8. Review table preview and summary.
9. Download Excel output.

## Troubleshooting

- `ModuleNotFoundError: gradio` or `pandas`
  - install dependencies from `requirements.txt`.
- Oracle connection errors
  - verify `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`.
- Bind variable errors
  - provide values implied by request and placeholders, especially `:report_period`.
- Missing metadata file
  - provide the selected agent metadata file under `metadata/agents/` or inspect generated schema stub.
- Agent metadata is empty
  - populate selected agent metadata file under `metadata/agents/` with valid documents.

## Change Management

Before changing code:

1. Read this `README.md`.
2. Read `forhumans.md` and preserve workflow assumptions.
3. Keep SQL safety constraints and artifact outputs intact.
4. Run at least compile checks after edits.
