# AGENTS.md

This file defines mandatory behavior for any human or AI agent modifying this repository.

## Mandatory First Step

Before making any change, you must:

1. Read `README.md` fully.
2. Read `forhumans.md` for project-specific prompting rules.
3. Confirm your planned change does not break the documented architecture and flow.
4. Understand the repository flow end-to-end before writing any code. If understanding is incomplete, do not start coding.

If you skip these, do not proceed with edits.

## Non-Disruption Rules

Do not disrupt existing behavior while adding features. Preserve:

1. End-to-end flow: request -> extraction -> metadata retrieval -> 3 SQL options -> selection -> Oracle execution -> preview/summary/excel.
2. Safety constraints in SQL generation and execution.
3. Artifact persistence under `runs/<timestamp>/`.
4. Core function contracts documented in `README.md`.
5. Environment-variable based secret handling.

## Required Validation After Changes

Run these checks after edits:

1. `py -m compileall app.py talk_to_data`
2. If dependencies exist locally, run app smoke test:
   - `py app.py`
3. Verify UI still shows 3 options and selection-run flow works.

## Documentation and Backlog Rules

1. Document every change in repository documentation. At minimum, update `AGENTS.md` with what changed.
2. Every agent must add a backlog entry in `AGENTS.md` for each work session.
3. Backlog entries must include date, agent name, and a short list of operations performed (reads, edits, commands, tests).
4. Agents must know and follow repository documentation (`README.md`, `forhumans.md`, `AGENTS.md`) during implementation.
5. Agents must update documentation whenever behavior, configuration, or workflow changes.

## Issue Resolution Standards

1. Find and validate the root cause before proposing or implementing a fix.
2. Prefer root-cause fixes over symptom-only patches.
3. If a temporary workaround is required, explicitly document the limitation and follow-up action.

## Editing Guidelines

1. Keep modules separated by responsibility; do not collapse everything into one file unless explicitly requested.
2. Keep SQL defensive and Oracle-compatible.
3. Keep metadata retrieval token-efficient; do not pass full metadata blindly to LLM.
4. Avoid secret leakage in logs/errors.
5. Maintain ASCII unless file already requires otherwise.
6. Runtime LLM calls in `talk_to_data/` must use `talk_to_data/llm_client.py`; do not route runtime LLM traffic through `scripts/llm_prompt.py`.
7. SQL candidate generation must fail with an explicit error when exactly 3 valid candidates cannot be produced; do not fabricate fallback SQL candidates.

## When Adding New Features

1. Update `README.md` architecture report and implementation notes.
2. Document new env vars and run artifacts.
3. Keep backward compatibility for existing callbacks and service methods.

## Conflict Resolution

If a request conflicts with existing architecture:

1. Propose minimal change path.
2. Preserve current contracts where possible.
3. Explicitly document any breaking change in `README.md` before merging.

## Operations Backlog

- 2026-02-09 - Codex: Read `README.md`, `forhumans.md`, `talk_to_data/db.py`, `talk_to_data/config.py`, `requirements.txt`, `AGENTS.md`. Attempted read of `talk_to_data/sql_explainer.py` was aborted. Ran `py -m compileall app.py talk_to_data`. Checked imports for `gradio`, `pandas`, `openpyxl`, `oracledb` and found `openpyxl` missing.
- 2026-02-09 - Codex: Listed repo files via `rg --files`. Read `app.py`, `scripts/llm_prompt.py`, `talk_to_data/llm_client.py`, `talk_to_data/requirements_extractor.py`, `talk_to_data/metadata_retriever.py`, `talk_to_data/sql_generator.py`, `talk_to_data/sql_explainer.py`, `talk_to_data/pipeline.py`, `talk_to_data/sql_guardrails.py`, `talk_to_data/summarizer.py`, `talk_to_data/runs.py`, `talk_to_data/__init__.py`.
- 2026-02-09 - Codex: Implemented local `.env` secret flow. Added `.env` to `.gitignore`, created root `.env` template with `OPENAI_API_KEY`, updated `talk_to_data/config.py` to load `.env` and accept `OPENAI_API_KEY` alias, removed hardcoded API key fallback in `scripts/llm_prompt.py`, added `python-dotenv` to `requirements.txt`, and updated `README.md` env var docs. Ran `py -m compileall app.py talk_to_data` (pass) and `py app.py` smoke test (startup reached Gradio launch; failed due port 7860 already in use).
- 2026-02-09 - Codex: Read `README.md`, `forhumans.md`, `scripts/llm_prompt.py`, `talk_to_data/requirements_extractor.py`, `talk_to_data/metadata_retriever.py`, `talk_to_data/sql_generator.py`, `talk_to_data/summarizer.py`, `talk_to_data/pipeline.py`, `talk_to_data/config.py`, `AGENTS.md`. Edited `talk_to_data/requirements_extractor.py` to route extraction prompts through `scripts/llm_prompt.py` and strengthen mandatory-filter prompting/normalization. Edited `talk_to_data/sql_generator.py` to generate/repair SQL through `scripts/llm_prompt.py` with fallback, pass request+metadata+required filters explicitly, and enforce required filters in `WHERE`. Edited `talk_to_data/metadata_retriever.py` to normalize mandatory filters and switch tokenization to Unicode-aware `\\w` with `re.UNICODE`. Edited `talk_to_data/summarizer.py`, `talk_to_data/config.py`, and `talk_to_data/pipeline.py` to add optional feature-flagged LLM result summarization step (`LLM_SUMMARIZER_ENABLED`) with heuristic fallback. Updated `README.md` for architecture/env/flow changes. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (failed due port 7860 in use), and validated generation path returns 3 SQL options via `py -` one-shot call to `generate_sql_options`.
- 2026-02-10 - Codex: Read `README.md`, `forhumans.md`, `talk_to_data/sql_generator.py`, `talk_to_data/metadata_retriever.py`, `talk_to_data/pipeline.py`, `AGENTS.md`. Edited `talk_to_data/metadata_retriever.py` for higher-recall retrieval (`top_k` default increased, broader column carry-over, debug fields). Edited `talk_to_data/pipeline.py` to call metadata retrieval with `top_k=20`. Edited `talk_to_data/sql_generator.py` to use requested prompt structure (`Metadata`, `Request`, `Sql Rule`) including mandatory filters and `FETCH FIRST 200 ROWS ONLY`, and made SQL candidate parsing tolerant to JSON/list/text variants while preserving 3-candidate contract. Updated `README.md` module notes. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (failed due port 7860 already in use), and validated generation path returns 3 non-empty SQL options via `py -` call to `generate_sql_options`.
- 2026-02-10 - Codex: Read `AGENTS.md` and added mandatory standards: full repository understanding before coding, root-cause-first issue resolution, and explicit documentation ownership. Updated operations backlog, ran `py -m compileall app.py talk_to_data` (pass), and ran `py app.py` smoke test (failed due port 7860 already in use).
- 2026-02-10 - Codex: Read `README.md`, `forhumans.md`, `AGENTS.md`, `scripts/llm_prompt.py`, `talk_to_data/config.py`, `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/sql_explainer.py`, `talk_to_data/summarizer.py`, `talk_to_data/pipeline.py`, and `app.py`. Root-caused missing `.env` key detection when `python-dotenv` is unavailable. Edited `scripts/llm_prompt.py` to add robust local `.env` fallback loading, env-based URL/model/timeout resolution, timeout usage in HTTP call, and placeholder key (`sk-xxxx`) validation. Edited `talk_to_data/config.py` to add the same `.env` fallback loading so `AppConfig.from_env()` resolves keys without dotenv. Updated `README.md` LLM env configuration notes. Ran `py -m compileall app.py talk_to_data scripts/llm_prompt.py` (pass), ran `py app.py` smoke test (failed due port 7860 already in use), validated `.env` key load in `scripts.llm_prompt` and `AppConfig.from_env()`, and validated generation path still returns 3 non-empty SQL options via `generate_sql_options`.
- 2026-02-10 - Codex: Read `README.md`, `forhumans.md`, `talk_to_data/sql_generator.py`, `talk_to_data/metadata_retriever.py`, `talk_to_data/pipeline.py`, `scripts/llm_prompt.py`, `AGENTS.md`. Edited `talk_to_data/sql_generator.py` to remove `INVALID_REQUEST` dependence in SQL generation path, require SQL-only 3-query model output, add robust raw SQL statement extraction/parsing, and add LLM normalization recovery for malformed outputs before app formatting. Edited `talk_to_data/metadata_retriever.py` and `talk_to_data/pipeline.py` to run high-recall retrieval with `top_k=200` (cap 200). Updated `README.md` to document SQL-only normalization flow and top-200 retrieval semantics. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (failed due port 7860 already in use), and ran `py -` one-shot checks for parser recovery and retrieval debug (`effective_top_k=200`).
- 2026-02-10 - Codex: Read `README.md`, `forhumans.md`, `talk_to_data/config.py`, `talk_to_data/llm_client.py`, `talk_to_data/pipeline.py`, `talk_to_data/sql_explainer.py`, `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/summarizer.py`, `.gitignore`, and `AGENTS.md`. Added `talk_to_data/llm_logging.py` and integrated prompt logging into `talk_to_data/llm_client.py` and `scripts/llm_prompt.py` so every outbound LLM prompt is logged. Updated `README.md` with new module, log artifact (`runs/llm_prompts.log`), and `LLM_PROMPT_LOG_PATH`. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (failed due port 7860 already in use), validated option generation still returns 3 choices via `generate_sql_options`, and verified prompt log entries are written by both LLM call paths.
- 2026-02-12 - Codex: Read `README.md`, `forhumans.md`, `AGENTS.md`, `talk_to_data/metadata_retriever.py`, `talk_to_data/sql_generator.py`, `talk_to_data/pipeline.py`, and `metadata_vectored.json`. Edited `talk_to_data/metadata_retriever.py` to normalize column metadata keys (`Keywords` -> `keywords`, `Type` -> `semantic_type`), carry column descriptions/properties/keywords in compact metadata, enrich retrieval search text with column semantic fields, and emit `retrieval_debug.metadata_source`. Edited `talk_to_data/sql_generator.py` to include metadata source and column-level descriptions/properties in SQL-generation prompt text. Edited `talk_to_data/pipeline.py` to pass configured metadata path into retrieval for source tracing. Updated `README.md` with prompt/context and metadata-source documentation. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (failed due port 7860 already in use), ran `py -` synthetic prompt rendering check (pass), and ran `py -` load check for `metadata_vectored.json` (failed: file currently invalid JSON, parse error at line 14).
- 2026-02-12 - Codex: Read `README.md`, `forhumans.md`, `AGENTS.md`, and `metadata_vectored.json`. Fixed `metadata_vectored.json` JSON syntax and encoding issues (added missing comma in `Keywords` list and rewrote file as UTF-8 without BOM). Validated with `py -` JSON parse check (pass for `metadata_vectored.json`), ran `py -m compileall app.py talk_to_data` (pass), and ran `py app.py` smoke test (failed due port 7860 already in use).
- 2026-03-11 - Codex: Read `README.md`, `forhumans.md`, `AGENTS.md`, `app.py`, `talk_to_data/config.py`, `talk_to_data/pipeline.py`, `talk_to_data/runs.py`, and `talk_to_data/metadata_retriever.py`. Added multi-agent registry module `talk_to_data/agent_registry.py`; added `metadata/agents/agents.json` and empty stub files `metadata_vectored_hasar.json`, `metadata_vectored_uretim.json`, `metadata_vectored_satis.json`; updated `config`, `pipeline`, `runs`, and `app` for agent dropdown selection + agent-specific metadata loading + `agent_info.json` artifact persistence; updated `README.md` for agent registry/layout/env/UI flow docs. Ran `py -m compileall app.py talk_to_data` (pass), ran `py app.py` smoke test (timed out due long-running server), ran `py -` `build_app()` smoke check (pass), ran `py -` service check for `list_agents()` and per-agent `prepare_candidates()` stub-error behavior (pass), and ran `py -` check for `generate_sql_options(..., 'hasar')` returning clear metadata-empty generation failure.
- 2026-03-12 - Codex: Read `README.md`, `forhumans.md`, `AGENTS.md`, `talk_to_data/requirements_extractor.py`, `talk_to_data/sql_generator.py`, `talk_to_data/sql_explainer.py`, `talk_to_data/summarizer.py`, and `talk_to_data/pipeline.py`. Removed runtime `scripts/llm_prompt.py` call paths from `talk_to_data/` modules and routed LLM usage through `talk_to_data/llm_client.py`. Removed SQL candidate fallback fabrication path and enforced explicit SQL-generation error when 3 valid candidates cannot be produced. Updated `README.md` and `AGENTS.md` documentation for new LLM-path and SQL-generation-failure behavior. Ran `py -m compileall app.py talk_to_data scripts/llm_prompt.py`; ran `py app.py` smoke test; ran `py -` targeted generation checks for no-LLM and malformed-output error paths.
