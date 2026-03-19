---
description: "Metadata veya agent konfigürasyonu değişikliği yaparken kullanılacak prompt"
---
# Agent / Metadata Configuration Change

Bu prompt, agent registry, metadata veya rules dosyalarında değişiklik yaparken izlenecek kuralları sağlar.

## Pre-Work

1. `metadata/agents/agents.json` registry dosyasını oku.
2. Değişiklik yapılan agent'ın dokümanını oku: `docs/agents/<agent>.md`
3. Genel agent doküman index'ini kontrol et: `docs/agents/README.md`

## Agent System Structure

```
metadata/agents/
├── agents.json                    # Agent registry
├── metadata_vectored_<agent>.json # Per-agent metadata
├── table_metadata_<agent>.json    # Per-agent table metadata
└── rules/
    ├── hasar.json                 # Per-agent SQL rules
    ├── uretim.json
    └── satis.json
```

## Rules

- Her agent'ın `id`, `label`, `metadata_path`, `table_metadata_path`, `rules_path` alanları olmalı.
- Agent metadata JSON dosyaları valid JSON olmalı ve document-shape validation'dan geçmeli.
- Per-agent rules dosyaları `metadata/agents/rules/` altında olmalı.
- Uretim agent'ı için time filter globally mandatory DEĞİLDİR.
- Yeni agent eklerken `docs/agents/<agent>.md` dokümanı da oluştur.

## Validation

```powershell
py -m compileall app.py talk_to_data
py -c "from app import build_app; build_app(); print('ok')"
```

## Documentation

- [ ] `docs/agents/<agent>.md` güncelle veya oluştur
- [ ] `docs/agents/README.md` index güncelle (yeni agent varsa)
- [ ] `AGENTS.md` backlog güncellemesi
- [ ] `README.md`'de agent listesi güncellemesi (yeni agent varsa)
