# AGENTS.md

This file defines mandatory behavior for any human or AI agent modifying this repository.

## Mandatory First Step

Before making any change, you must:

1. Read `README.md` fully.
2. Read `forhumans.md` for project-specific prompting rules.
3. Read `architecture.md` fully and understand the documented architecture.
4. Confirm your planned change does not break the documented architecture and flow.
5. Understand the repository flow end-to-end before writing any code. If understanding is incomplete, do not start coding.

If you skip these, do not proceed with edits.

## Non-Disruption Rules

Do not disrupt existing behavior while adding features. Preserve:

1. End-to-end flow: request -> extraction -> metadata retrieval -> 3 SQL options -> best-option selection (LLM judge + fallback) -> Oracle execution -> preview/summary/excel.
2. Safety constraints in SQL generation and execution.
3. Artifact persistence under `runs/<timestamp>/`.
4. Core function contracts documented in `README.md`.
5. Environment-variable based secret handling.

## Required Validation After Changes

Run these checks after edits:

1. `py -m compileall app.py talk_to_data`
2. If dependencies exist locally, run app smoke test:
   - `py -c "from app import build_app; build_app(); print('ok')"`
3. Verify UI still shows 3 options, auto-selection works, and execution flow works.

## Documentation and Backlog Rules

1. Document every change in repository documentation. At minimum, update `AGENTS.md` with what changed.
2. Every agent must add a backlog entry in `AGENTS.md` for each work session.
3. Backlog entries must include date, agent name, and a short list of operations performed (reads, edits, commands, tests).
4. Agents must know and follow repository documentation (`README.md`, `forhumans.md`, `architecture.md`, `AGENTS.md`) during implementation.
5. Agents must update documentation whenever behavior, configuration, workflow, or architecture changes.
6. If architecture, flow, module responsibilities, or core contracts change, update `architecture.md` in the same session.
7. If architecture changes, add an entry to `architecture.md` under `Architecture Change Log (Mandatory)`.

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

## Architecture Discipline

1. Every agent must read and understand `architecture.md` before development.
2. `architecture.md` is the canonical architecture source of truth.
3. Every architecture-impacting change must be documented in `architecture.md`.
4. Architecture-impacting work is incomplete unless the change log in `architecture.md` is updated.
5. If implementation and `architecture.md` diverge, resolve in the same session by either updating code to the documented design or updating `architecture.md` to the actual runtime behavior before continuing.


## Agent Documentation Map (Mandatory)

Use per-agent documentation before changing any agent-specific logic or policy.

- Index: `docs/agents/README.md`
- Hasar: `docs/agents/hasar.md`
- Uretim: `docs/agents/uretim.md`
- Satis: `docs/agents/satis.md`

Policy note (Uretim):
- Time filter is NOT globally mandatory for `uretim`.
- Do not fail requests only because there is no explicit time filter.
- Apply time predicates when request explicitly includes a period/time scope.

## Operations Backlog

| Date | Agent | Summary |
|------|-------|---------|
| 2026-03-11 | Codex | Added multi-agent registry (`agent_registry.py`), `agents.json`, stub metadata files; wired agent dropdown in UI and pipeline |
| 2026-03-12 | Codex | Routed runtime LLM calls through `llm_client.py`; enforced explicit error on <3 SQL candidates (no fallback fabrication) |
| 2026-03-13 | Codex | Fixed empty-metadata error; added `sql_judge.py` (LLM judge + deterministic fallback); auto-run recommended SQL; validated uretim metadata; audited Excel-to-JSON completeness; implemented uretim period-policy flow |
| 2026-03-14 | Codex | Added post-query Turkish interpretation with chart plan; added `architecture.md`; added `sql_validation.py` shared validator with alias.column blocking |
| 2026-03-15 | Codex | Time-granularity extraction; tanzim-path date routing; Oracle bind mapping; TO_CHAR guardrails; chart plan validation; `LLM_SUMMARIZER_REQUIRED` gate; reworked SQL validation for quoted identifiers and schema ambiguity |
| 2026-03-16 | Codex | Bind-resolved SQL preview in UI; validation+repair+revalidation for filters; metadata join-key quality gate; judge retry orchestration; per-agent docs under `docs/agents/` |
| 2026-03-17 | Codex | Removed join-key fail-fast; architecture audit and `architecture.md` sync; retrieval top-500; removed extractor-side transforms (validation-only SQL, no mutation) |
| 2026-03-18 | Codex | Resolved merge conflicts; per-agent rules (`agent_rules.py`, `rules/*.json`); prompt-only SQL policy; `prompt_budget.py` with CTE-aware extraction; tightened SQL-gen prompt; batched SQL explainer; `llm_usage.json` artifact |
| 2026-03-19 | Codex | Dual agent metadata model (`table_metadata.py`); split structural/table metadata; runtime merge; relaxed unknown column checks for join-declared keys |
| 2026-03-19 | Copilot | Created `.github/copilot-instructions.md` and reusable prompt files; pruned AGENTS.md backlog to structured changelog |
