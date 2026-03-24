# Copilot Instructions — Talk to Your Data

Bu dosya, GitHub Copilot Chat'in bu workspace'te her etkileşimde otomatik olarak okuduğu talimat dosyasıdır.

## Mandatory First Step

Herhangi bir değişiklik yapmadan önce şu dosyaları oku ve anla:

1. `AGENTS.md` — tüm AI agent'lar için zorunlu davranış kuralları.
2. `forhumans.md` — proje bazlı prompting kuralları.
3. `architecture.md` — canonical mimari kaynak (end-to-end flow, modül sorumlulukları, kontratlar).
4. `README.md` — proje genel bakış ve modül dokümantasyonu.

Eğer anlayış eksikse kod yazmaya başlama.

## Project Overview

Natural-language analytics isteklerini güvenli Oracle SQL seçeneklerine çeviren, en iyisini otomatik seçen, çalıştıran ve tablo + özet + Excel çıktısı döndüren bir Gradio uygulaması.

## End-to-End Flow (Do Not Break)

```
request → extraction → metadata retrieval → 3 SQL options → best-option selection (LLM judge + fallback) → Oracle execution → preview/summary/excel
```

## Core Architecture Rules

- Candidate generation must produce exactly 3 SQL options. Fabricating fallback SQL when 3 valid candidates can't be produced is forbidden — fail with an explicit error.
- Only `SELECT`/`WITH` queries are allowed. No DML/DDL, no `SELECT *`, no SQL comments.
- Oracle row limit: `FETCH FIRST 200 ROWS ONLY`.
- Artifacts persist under `runs/<timestamp>/`.
- Secrets are env-driven only — never hardcode API keys or passwords.
- Runtime LLM calls in `talk_to_data/` must use `talk_to_data/llm_client.py` — do not route through `scripts/llm_prompt.py`.
- SQL generation does not mutate/repair LLM SQL text; candidates are parse-only.
- Mandatory filter obligation enforcement is disabled globally.

## Module Map

| Module | Responsibility |
|--------|---------------|
| `app.py` | Gradio UI + callbacks |
| `talk_to_data/pipeline.py` | Main orchestrator (`TalkToDataService`) |
| `talk_to_data/config.py` | Env-based config + `.env` loading |
| `talk_to_data/agent_registry.py` | Multi-agent registry (`metadata/agents/agents.json`) |
| `talk_to_data/agent_rules.py` | Per-agent SQL prompt rules |
| `talk_to_data/requirements_extractor.py` | Structured requirement extraction |
| `talk_to_data/metadata_retriever.py` | High-recall metadata retrieval (top 500) |
| `talk_to_data/sql_generator.py` | 3-candidate SQL generation |
| `talk_to_data/sql_explainer.py` | SQL explanation (optional batched LLM) |
| `talk_to_data/sql_judge.py` | LLM judge + deterministic fallback |
| `talk_to_data/sql_guardrails.py` | Execution-time safety validation |
| `talk_to_data/sql_validation.py` | Metadata-backed column/alias validation |
| `talk_to_data/db.py` | Oracle driver + bind resolution |
| `talk_to_data/summarizer.py` | Result summary (heuristic/LLM) + chart plan |
| `talk_to_data/runs.py` | Run artifact persistence |
| `talk_to_data/llm_client.py` | OpenAI-compatible LLM wrapper |
| `talk_to_data/llm_logging.py` | JSONL prompt logging |
| `talk_to_data/prompt_budget.py` | Prompt-budget metadata serializers |
| `scripts/llm_prompt.py` | CLI-only LLM prompt utility (not for runtime) |

## Multi-Agent System

- Agents: `hasar`, `uretim`, `satis` (registry: `metadata/agents/agents.json`)
- Her agent'ın kendine ait metadata, table metadata ve rules dosyası var.
- Agent-specific logic değiştirmeden önce `docs/agents/<agent>.md` dosyasını oku.
- Uretim agent'ı için time filter globally mandatory DEĞİLDİR — sadece request'te açık zaman kapsamı varsa uygula.

## Coding Standards

- Modülleri sorumluluk bazlı ayrı tut — tek dosyaya sıkıştırma.
- SQL defensive ve Oracle-compatible olsun.
- Metadata retrieval token-efficient olsun — full metadata'yı LLM'e gönderme.
- Log/error çıktılarında secret sızıntısından kaçın.
- ASCII kullan (dosya zaten gerektirmiyorsa).
- Mevcut callback ve service method contract'larını koru.

## Validation After Changes

Değişiklik sonrası şu kontrolleri yap:

```powershell
py -m compileall app.py talk_to_data
py -c "from app import build_app; build_app(); print('ok')"
```

## Documentation Rules

- Her değişiklik `AGENTS.md` backlog tablosuna tek satır olarak eklenmeli: `| YYYY-MM-DD | agent | summary |`. Tarih başına agent başına max bir satır. Okunan dosyaları ve validation komutlarını listeleme — sadece ne değiştiğini yaz.
- Mimari değişiklikler `architecture.md` change log'una da eklenmeli.
- Yeni env var'lar ve artifact'lar `README.md`'de dokümante edilmeli.

## Key Config Env Vars

- `LLM_API_KEY` / `OPENAI_API_KEY`, `LLM_URL`, `LLM_MODEL`, `LLM_TIMEOUT_SEC`
- `ORACLE_USER`, `ORACLE_PASSWORD`, `ORACLE_DSN`
- `METADATA_VECTORED_PATH`, `AGENT_REGISTRY_PATH`, `RUNS_DIR`
- `LLM_SUMMARIZER_ENABLED`, `SQL_EXPLAINER_ENABLED`, `RESULT_CHART_RENDER_ENABLED`
