# Talk to Your Data

Production-minded Gradio app that converts natural language requests into safe Oracle SQL options, automatically chooses the best one, executes it, and returns tabular + textual + Excel outputs.

## Scope

This repository implements the end-to-end workflow requested in `forhumans.md`:

1. Take user request from UI.
2. Extract structured requirements (Prompt Part 2.5 style).
3. Retrieve high-recall relevant metadata from selected agent metadata file.
4. Generate exactly 3 SQL candidates.
5. Save run artifacts into timestamped folders.
6. Explain each SQL in plain language.
7. LLM judge selects the best SQL candidate (hard-disqualify aware with deterministic fallback).
8. Execute the selected SQL automatically on Oracle.
9. Interpret query results in Turkish (optional LLM step with heuristic fallback) and generate chart plan payload.
10. Show result preview, summary text, chart plan (deactivated), and Excel download.

## Architecture Report

### High-Level Flow

1. `app.py` receives agent selection + natural-language request.
2. `TalkToDataService.prepare_candidates()` orchestrates:
   - `load_agent_registry()` + agent resolution
   - `extract_requirements()`
   - `retrieve_relevant_metadata()`
   - `generate_sql_candidates()`
   - `describe_sql_candidate()`
   - `choose_best_sql_candidate()`
   - artifact persistence under `runs/<timestamp>/`
3. UI displays 3 candidate cards and highlights auto-selected candidate.
4. `TalkToDataService.execute_selected_candidate()` auto-runs the recommended candidate:
   - executes SQL using Oracle driver and bind params
   - interprets result in Turkish and emits chart plan payload (LLM optional via feature flag)
   - keeps chart rendering deactivated by default
   - writes `result.xlsx`
5. UI returns status, preview table, summary text, chart plan text, and download file.

### Module Responsibilities

- `app.py`
  - Gradio layout and callbacks.
  - Generates options and runs selected SQL.
  - Displays only final SQL preview (bind-resolved, display-only) in option cards and run status.
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
  - Extracts normalized `time_granularity` + `time_value` from `YYYY`, `YYYYMM`, `YYYYMMDD` and separated forms (`YYYY-MM`, `YYYY-MM-DD`).
  - Enforces calendar validation for explicit day tokens; invalid `YYYYMMDD` / `YYYY-MM-DD` requests fail fast with extraction error.
  - Applies metadata-aware period policy: does not auto-inject `REPORT_PERIOD` filters when selected metadata indicates a different time basis (for example `uretim` ek tanzim policy).
  - Heuristic fallback when LLM is unavailable.
- `talk_to_data/metadata_retriever.py`
  - Loads selected agent metadata JSON.
  - Validates JSON/document structure and normalizes metadata payload for retrieval.
  - High-recall retrieval using Unicode-aware token cosine similarity.
  - Returns broader relevant payload (more tables/columns) and mandatory rules.
  - Initializes `runtime_mandatory_rules` container for generation-time obligations without mutating static metadata obligations.
  - Builds metadata overview hints (`has_report_period_column`, `time_filter_policy`) for extractor prompts.
  - Supports high-recall retrieval up to top 200 metadata documents per request.
- `talk_to_data/sql_generator.py`
  - Implements `generate_sql_candidates(...) -> list[dict]`.
  - Produces exactly 3 candidates.
  - Uses `talk_to_data/llm_client.py` for LLM calls.
  - Accepts optional retry context (`retry_context`) so second-attempt prompts can avoid first-attempt disqualify patterns.
  - Builds LLM prompt with explicit `Metadata`, `Request`, and `Sql Rule` sections.
  - Builds granularity-aware mandatory filters for DATE/TIMESTAMP targets:
    - `TO_CHAR(<date_col>, 'yyyy') = :year_value`
    - `TO_CHAR(<date_col>, 'yyyymm') = :report_period`
    - `TO_CHAR(<date_col>, 'yyyymmdd') = :date_value`
  - Supports controlled date-like NUMBER targets (strong metadata signals only) with digit-safe predicate generation.
  - If request signals tanzim period intent, prioritizes `TANZIM_TARIH_ID -> GNL_TARIH.TARIH` route while selecting date target from metadata.
  - Includes column-level context in prompt (`type`, `description`, `semantic_type`, `keywords`, selected properties when available).
  - Carries metadata source trace (`retrieval_debug.metadata_source`) into prompt for auditability.
  - Requests SQL-only output first, then normalizes/parses into app candidate format.
  - Applies SQL safety checks and regeneration/repair path.
  - Does not inject missing predicates directly into SQL text; enforces mandatory filters through validation + LLM repair + revalidation.
  - Enforces strict granularity bind policy:
    - `year` -> `:year_value` only
    - `month` -> `:report_period` only
    - `day` -> `:date_value` only
  - Fails with explicit error when model output cannot be normalized into 3 valid SQL candidates (no synthetic fallback SQL).
  - Normalizes mandatory filters (for example `REPORT_PERIOD` -> `REPORT_PERIOD = :report_period`) before enforcement.
  - Stores generation-time obligations in `runtime_mandatory_rules` to avoid mutating static `mandatory_rules`.
  - Enforces `uretim`-only ek tanzim period policy during candidate validation/repair (rejects `REPORT_PERIOD` column predicates when ek tanzim basis is active).
  - Enforces Oracle row limit style `FETCH FIRST 200 ROWS ONLY`.
- `talk_to_data/sql_guardrails.py`
  - Execution-time SQL validation before Oracle run.
  - Re-checks safety, table allowlist, alias/column metadata compatibility, and mandatory filter obligations.
  - Validates granular date obligations (`:year_value`, `:report_period`, `:date_value`) with bind+mask aware pattern checks, including `TO_CHAR` variants with `TRUNC/CAST`.
  - Evaluates both static `mandatory_rules` and runtime-generated `runtime_mandatory_rules`.
  - Accepts optional full `validation_catalog` for execution-time column checks.
- `talk_to_data/sql_validation.py`
  - Builds full table-column validation catalog from raw metadata documents.
  - Parses quoted/unquoted references (`a.c`, `"a"."c"`, `table.col`, `"SCHEMA"."TABLE"."COL"` last two parts).
  - Detects ambiguous bare-table references and unknown alias.column references.
- `talk_to_data/sql_judge.py`
  - Evaluates 3 SQL candidates and picks best option id.
  - Uses strict LLM judge prompt with `temperature=0.0` and `max_tokens=32`.
  - Applies deterministic fallback (hard disqualify + local scoring + fewer-disqualify tie-break + stable option order).
  - Emits judge outcome metadata: `judge_error_kind`, `all_candidates_disqualified`, `disqualified_count`, `retry_recommended`.
- `talk_to_data/sql_explainer.py`
  - Implements `describe_sql_candidate(...) -> str`.
- `talk_to_data/db.py`
  - Oracle execution and bind variable preparation.
  - Supports `:report_period`, `:year_value`, `:date_value`, date-range binds, and row-limit bind aliases.
  - Provides shared SQL display renderer (`render_sql_for_display`) so UI preview matches bind mapping logic.
  - Sanitized error messages (no secret leakage).
- `talk_to_data/summarizer.py`
  - Implements `summarize_result(...) -> ResultInterpretation` and compatibility wrapper `summarize_result_to_text(df) -> str`.
  - Produces Turkish summary plus structured `chart_plan` payload.
  - Optional LLM interpretation path controlled by `LLM_SUMMARIZER_ENABLED`; heuristic fallback remains default.
  - Supports strict mode via `LLM_SUMMARIZER_REQUIRED` (pipeline raises controlled error if LLM summary is mandatory but unavailable).
  - Validates chart plan against result dataframe and emits `validation_errors` on invalid plan.
  - Chart rendering can be toggled by `RESULT_CHART_RENDER_ENABLED` and is disabled by default.
- `talk_to_data/runs.py`
  - Run folder creation + JSON/text/Excel persistence.
- `talk_to_data/pipeline.py`
  - End-to-end application service orchestration.
  - Builds and carries full metadata validation catalog for judge + execution guardrails.
  - Runs generate+judge flow with at most one retry (`attempt_1` + `attempt_2`) when judge fails or all options are disqualified.
  - Fail-fast blocks generation if second attempt still has all candidates disqualified.
  - Persists retry observability artifacts when retry is used.
  - Exposes `list_agents()` and agent-aware `prepare_candidates(...)`.

### Core Data Contracts

- `extract_requirements(user_request) -> dict`
  - includes: `intent`, `required_filters`, `measures`, `dimensions`, `grain`, `time_range`, `report_period`, `time_granularity`, `time_value`, `join_needs`, `row_limit`, `security_constraints`.
- `retrieve_relevant_metadata(requirements, user_request) -> dict`
  - includes: `relevant_items`, `guardrails`, `mandatory_rules`, `runtime_mandatory_rules`.
- `generate_sql_candidates(user_request, requirements, metadata, llm_client, retry_context=None) -> list[dict]`
  - each: `{id, sql, rationale_short, risk_notes}`.
- `describe_sql_candidate(candidate, metadata) -> str`
- `select_best_sql_option_id(user_request, metadata_used, candidates, llm_client=None, validation_catalog=None) -> str`
  - Judge details include: `judge_error_kind`, `all_candidates_disqualified`, `disqualified_count`, `retry_recommended`.
- `summarize_result_to_text(df) -> str`
  - optional args: `user_request`, `sql`, `metadata_used`, `llm_client`, `llm_enabled`, `chart_render_enabled`
- `summarize_result(df) -> ResultInterpretation`
  - fields: `summary_text`, `chart_plan`, `llm_used`, `chart_render_enabled`, `summary_mode`, `fallback_reason`, `validation_errors`
- `TalkToDataService.prepare_candidates(user_request, agent_id=None) -> dict`
- `TalkToDataService.list_agents() -> list[dict[str, str]]`

## Safety and Guardrails

- Only SELECT/CTE statements are allowed.
- Blocked keywords include: `DROP`, `DELETE`, `INSERT`, `UPDATE`, `MERGE`, `ALTER`, `TRUNCATE`.
- Multiple statements are blocked.
- `SELECT *` is blocked.
- Row limit is always enforced with `FETCH FIRST 200 ROWS ONLY`.
- Mandatory metadata filters are propagated and enforced.
- Granular time filters can be enforced via `TO_CHAR` predicates with binds (`:year_value`, `:report_period`, `:date_value`) when DATE/TIMESTAMP targets are available.
- Controlled date-like NUMBER columns can be used for granular time filters only with strong metadata signals.
- Granularity bind policy is strict (`year -> :year_value`, `month -> :report_period`, `day -> :date_value`).
- Bare mandatory filter names are normalized to bind-safe predicates before SQL generation.
- SQL output normalization rejects `INVALID_REQUEST` marker responses and keeps SQL-generation flow active.
- SQL generation fails fast when 3 valid candidates cannot be produced after normalization/repair.
- Missing mandatory filters are handled via validation+repair, not SQL text injection.
- `uretim` metadata only: when guardrails indicate ek tanzim period basis, SQL generation blocks `REPORT_PERIOD` column filters and requires ek tanzim date context with `:report_period`.
- Execution-time guardrails validate SQL `alias.column` references against metadata-derived table-column sets.
- Execution-time guardrails use full metadata validation catalog (when provided) instead of compact top-60 columns.
- Ambiguous bare table references (same table name in multiple schemas) are blocked with explicit candidate-table details.
- SQL judge output is parsed with regex for `option_[123]`; if parse fails, deterministic fallback is applied.
- Bind placeholders are used for runtime values (`:report_period`, `:year_value`, `:date_value`, date-range binds, etc.).
- Oracle errors are sanitized to avoid leaking secrets.

## Run Artifacts

Each generation creates `runs/<timestamp>/` with:

- `requirements.json`
- `metadata_used.json`
- `sql_candidates.json`
- `judge_result.json`
- `sql_candidates_attempt_1.json` (retry path only)
- `judge_result_attempt_1.json` (retry path only)
- `retry_decision.json` (retry path only)
- `request.txt`
- `agent_info.json` (when generation is agent-based)

Each execution also writes:

- `result.xlsx`
- `result_preview.csv`
- `result_interpretation.json` (Turkish summary + chart plan payload + `summary_mode` + `fallback_reason` + `validation_errors` + render flag)

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
- `talk_to_data/sql_validation.py`
- `talk_to_data/sql_judge.py`
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

- App starts on the first available Gradio port (typically `7860`).
- If you need a fixed port, set `GRADIO_SERVER_PORT` before launch.

## Environment Variables

LLM:

- `LLM_API_KEY` (required for LLM mode)
- `OPENAI_API_KEY` (accepted alias for local `.env` usage)
- `LLM_URL` (optional; default provided in code)
- `LLM_MODEL` (optional; default `florence_v2`)
- `LLM_TIMEOUT_SEC` (optional; default `60`)
- `LLM_SUMMARIZER_ENABLED` (optional; `true/1/on` enables LLM result summarization step)
- `LLM_SUMMARIZER_REQUIRED` (optional; `true/1/on` makes LLM summary mandatory; execution fails with controlled error when fallback would be used)
- `RESULT_CHART_RENDER_ENABLED` (optional; `true/1/on` enables chart rendering path; default is disabled)
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
- `GRADIO_SERVER_PORT` (optional; when set, app binds to that fixed port)

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
   - SQL cards show final SQL preview with bind values resolved for display.
   - Raw template SQL with placeholders is not shown in UI.
6. System auto-selects best option and executes it immediately.
7. Review run status (includes selected final SQL preview), table preview, summary, and `Chart Plan (Deaktif)` panel.
8. (Optional) Override option via radio and click `Run Selected SQL` for manual rerun.
9. Download Excel output.

## Troubleshooting

- `ModuleNotFoundError: gradio` or `pandas`
  - install dependencies from `requirements.txt`.
- Oracle connection errors
  - verify `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`.
- Bind variable errors
  - provide values implied by request and placeholders, especially `:report_period`, `:year_value`, and `:date_value`.
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
