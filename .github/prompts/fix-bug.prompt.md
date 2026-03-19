---
description: "Bug fix yaparken kullanılacak root-cause analiz prompt'u"
---
# Bug Fix — Root Cause Analysis

Bu prompt, bug fix yaparken root-cause-first yaklaşımla çalışmayı sağlar.

## Mandatory Steps

1. **Root Cause Bul:** Semptomu değil, kök nedeni tespit et.
2. **Architecture Kontrol:** `architecture.md` oku ve fix'in mevcut kontratları bozmadığını onayla.
3. **Affected Modules:** Etkilenen modüllerin sorumluluklarını kontrol et:
   - `pipeline.py` → orchestration
   - `sql_generator.py` → SQL üretimi
   - `sql_guardrails.py` → güvenlik validation
   - `sql_judge.py` → LLM judge + fallback
   - `db.py` → Oracle execution
   - `app.py` → Gradio UI + callbacks

## Fix Rules

- Semptom-only patch yerine root-cause fix'i tercih et.
- Geçici workaround gerekiyorsa limitasyonu ve follow-up action'ı açıkça dokümante et.
- Mevcut callback ve service method contract'larını koru.
- SQL text'i mutate/repair etme — candidates parse-only.

## Validation

```powershell
py -m compileall app.py talk_to_data
py -c "from app import build_app; build_app(); print('ok')"
```

## Documentation

- [ ] `AGENTS.md` backlog güncellemesi
- [ ] Breaking change varsa `README.md`'de dokümante et
