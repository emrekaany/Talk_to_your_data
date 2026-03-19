---
description: "SQL pipeline modüllerinde (generator, judge, guardrails) çalışırken kullanılacak prompt"
---
# SQL Pipeline Work

Bu prompt, SQL generation/validation/execution pipeline'ında çalışırken izlenecek kuralları sağlar.

## Pipeline Flow

```
extract_requirements() → retrieve_relevant_metadata() → generate_sql_candidates() → describe_sql_candidates() → choose_best_sql_candidate() → validate_sql_before_execution() → execute on Oracle
```

## Critical Invariants

1. **3 Candidate Rule:** `generate_sql_candidates()` tam 3 geçerli SQL candidate döndürmeli. Fallback fabrication yasak.
2. **Parse-Only:** SQL generation LLM output'unu mutate/repair etmez. Candidates parse-only normalize edilir.
3. **Safety Guardrails:** Execution öncesi `validate_sql_before_execution()` çalışmalı:
   - Sadece `SELECT`/`WITH` izinli
   - DML/DDL/multiple statements bloklı
   - `SELECT *` ve SQL comments bloklı
   - Table allowlist validation
   - Alias.column metadata validation
4. **Bind Resolution:** Oracle bind params requirements'tan çözümlenir (`db.py`).
5. **Row Limit:** `FETCH FIRST 200 ROWS ONLY` zorunlu.

## Module Responsibilities

- `sql_generator.py`: Prompt engineering + 3 candidate parse. Agent rules injection.
- `sql_guardrails.py`: Execution-time safety. Mandatory filter obligation disabled.
- `sql_judge.py`: LLM judge + deterministic fallback. Retry signaling.
- `sql_validation.py`: Metadata-backed catalog build, alias/column check, ambiguity detection.
- `sql_explainer.py`: Optional batched LLM explanation.
- `prompt_budget.py`: Compact metadata summaries for judge/explainer.

## LLM Call Rules

- Runtime LLM calls: `talk_to_data/llm_client.py` kullan.
- `scripts/llm_prompt.py` sadece CLI utility — runtime'da kullanma.

## Validation

```powershell
py -m compileall app.py talk_to_data
py -c "from app import build_app; build_app(); print('ok')"
```
