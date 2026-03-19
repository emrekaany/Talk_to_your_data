---
description: "Yeni feature eklerken kullanılacak kontrol prompt'u"
---
# New Feature Checklist

Bu prompt, repo'ya yeni feature eklerken adım adım izlenecek kontrol listesini sağlar.

## Mandatory Pre-Work

1. `AGENTS.md`, `architecture.md`, `README.md` ve `forhumans.md` dosyalarını oku.
2. Planlanan değişikliğin mevcut end-to-end flow'u bozmadığını onayla:
   ```
   request → extraction → metadata retrieval → 3 SQL options → best-option selection → Oracle execution → preview/summary/excel
   ```
3. Etkilenen modüllerin sorumluluklarını `architecture.md`'den kontrol et.

## Implementation Rules

- Modülleri sorumluluk bazlı ayrı tut.
- Runtime LLM çağrıları `talk_to_data/llm_client.py` üzerinden olmalı.
- SQL generation tam 3 candidate üretmeli; fallback SQL fabrication yasak.
- Sadece `SELECT`/`WITH` query'lere izin ver. DML/DDL, `SELECT *`, SQL comment yasak.
- Oracle row limit: `FETCH FIRST 200 ROWS ONLY`.
- Secret'lar sadece env-driven olmalı.

## Post-Implementation Validation

```powershell
py -m compileall app.py talk_to_data
py -c "from app import build_app; build_app(); print('ok')"
```

## Documentation Updates

- [ ] `AGENTS.md` backlog güncellemesi
- [ ] Mimari değişiklik varsa `architecture.md` change log güncellemesi
- [ ] Yeni env var / artifact varsa `README.md` güncellemesi
- [ ] Agent-specific değişiklik varsa `docs/agents/<agent>.md` güncellemesi
