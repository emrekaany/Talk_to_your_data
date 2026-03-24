"""One-time script: clean table_metadata_uretim.json

Fixes:
- Concatenated descriptions (14/20 tables) → single-table descriptions
- Column-level business_notes → removed (already in column_metadata)
- Olasi soru examples → removed
- Statistical/source artifacts → removed
- Contaminated grain → table-appropriate grain
- Contaminated keywords → own-column keywords only

Usage:
    python scripts/clean_table_metadata.py
"""

from __future__ import annotations

import json
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "metadata" / "agents" / "table_metadata_uretim.json"

# ── Per-table cleanup rules ────────────────────────────────────────────────

CLEAN_DESCRIPTION: dict[str, str] = {
    "AS_DWH.ACE_ACENTE": "Acentelere ait bilgileri tutan tablo.",
    "AS_DWH.ACE_ACENTE_ADRES": "Acentelere ait adres bilgilerini tutar.",
    "AS_DWH.ACE_SATIS_MUDURLUGU": "Acente bölgelerine ait bilgileri tutan tablo.",
    "AS_DWH.FACT_POL_POLICE_TEM_ASSET": "Poliçe Teminat (TKZ) seviyesinde bazı tutarsal bilgileri tutar.",
    "AS_DWH.GNL_KAYNAK_SISTEM": "Poliçe verisinin hangi kaynak sistemden (1-AS400/2-ASOS/15-ASTRA) geldiğini gösteren tablodur.",
    "AS_DWH.GNL_TARIH": "Genel Tarih Bilgileri Tutan Takvim Tablosu. Join üzerinden ilgili ID ile istenilen tarih bilgisi elde edilir.",
    "AS_DWH.MUS_MUSTERI": "Müşteri Bilgilerini İçeren Tablo",
    "AS_DWH.MUS_MUSTERI_CINSIYET": "Müşteri cinsiyet tanımlarını içeren look up tablo.",
    "AS_DWH.POL_BRANS": "Branş Tanımlarını İçeren Look Up Tablo",
    "AS_DWH.POL_POLICE_DURUMU": "Poliçenin Yürürlükte mi, İptal mi yoksa süresini mi tamamladı gibi durum bilgisini veren tablo.",
    "AS_DWH.POL_POLICE_KIRILIM": "Poliçe bazında branş ve TKZ kırılım bilgilerini tutan köprü tablo.",
    "AS_DWH.POL_POLICE_OZET": "Poliçe Bilgilerini Tutan Tablo",
    "AS_DWH.POL_POLICE_REF_TANIM": "Poliçe kodu, branş ve TKZ referans tanımlarını içeren tablo. Vergi Öncesi Prim TL hesaplamalarında kullanılır.",
    "AS_DWH.POL_POLICE_SATIS_KANALI": "Poliçe Satış Kanallarını İçeren Look Up Tablo",
    "AS_DWH.POL_URUN": "Ürünlere ait bilgileri tutan tablo.",
}

CLEAN_GRAIN: dict[str, str] = {
    "AS_DWH.ACE_ACENTE": "veri_seviyesi=Acente Kodu; degisim_seviyesi=Acente Kodu",
    "AS_DWH.ACE_ACENTE_ADRES": "veri_seviyesi=Acente Kodu; degisim_seviyesi=Acente Kodu",
    "AS_DWH.ACE_SATIS_MUDURLUGU": "veri_seviyesi=Satış Müdürlüğü; degisim_seviyesi=Satış Müdürlüğü",
    "AS_DWH.FACT_POL_POLICE_TEM_ASSET": "veri_seviyesi=Police ID; degisim_seviyesi=Police ID",
    "AS_DWH.GNL_KAYNAK_SISTEM": "veri_seviyesi=Kaynak Sistem; degisim_seviyesi=Kaynak Sistem",
    "AS_DWH.GNL_TARIH": "veri_seviyesi=Tarih; degisim_seviyesi=Tarih",
    "AS_DWH.MUS_MUSTERI": "veri_seviyesi=Müşteri; degisim_seviyesi=Müşteri",
    "AS_DWH.POL_BRANS": "veri_seviyesi=Branş; degisim_seviyesi=Branş",
    "AS_DWH.POL_POLICE_DURUMU": "veri_seviyesi=Poliçe Durumu; degisim_seviyesi=Poliçe Durumu",
    "AS_DWH.POL_POLICE_OZET": "veri_seviyesi=Poliçe; degisim_seviyesi=Poliçe; degisim_seviyesi=Poliçe Yenileme No; degisim_seviyesi=Poliçe Ek Numarası",
    "AS_DWH.POL_POLICE_REF_TANIM": "veri_seviyesi=Referans Tanım (REF_ID); degisim_seviyesi=Referans Tanım (REF_ID)",
    "AS_DWH.POL_POLICE_SATIS_KANALI": "veri_seviyesi=Satış Kanalı; degisim_seviyesi=Satış Kanalı",
    "AS_DWH.POL_TKZ": "veri_seviyesi=TKZ; degisim_seviyesi=TKZ",
    "AS_DWH.POL_URUN": "veri_seviyesi=Ürün; degisim_seviyesi=Ürün",
}

CLEAN_KEYWORDS: dict[str, list[str]] = {
    "AS_DWH.ACE_ACENTE": [
        "Üretim", "AS_DWH.ACE_ACENTE", "Kanal",
        "Acente Kodu", "Acenta Tanımlama Kodu", "Acenta Referans Kodu",
        "Acenta Numarası", "Satış Acentesi Kodu",
        "Acente Unvanı", "Acente Adı",
        "Acente Bölge Kodu", "Satış Müdürlüğü", "Satış Bölge Müdürlüğü", "Bölge Satış Birimi",
        "Acente Tipi", "Acente Türü", "Acente Sınıfı", "Acente Sınıflandırması",
        "Dağıtım Kanalı",
    ],
    "AS_DWH.ACE_ACENTE_ADRES": [
        "AS_DWH.ACE_ACENTE_ADRES", "Kanal",
        "Acente İl Adı", "Acente Şehri", "Acente İl Bilgisi", "Acente Konum İli",
    ],
    "AS_DWH.ACE_SATIS_MUDURLUGU": [
        "AS_DWH.ACE_SATIS_MUDURLUGU",
        "Acente Bölge Kodu", "Satış Müdürlüğü", "Satış Bölge Müdürlüğü", "Bölge Satış Birimi",
        "Acente Bölge Adı", "Acente Bölge İsmi", "Acentenin Bağlı Olduğu Bölge",
        "Satış Bölgesi Adı", "Acente Bölge Bilgisi",
    ],
    "AS_DWH.FACT_POL_POLICE_TEM_ASSET": [
        "Üretim", "AS_DWH.FACT_POL_POLICE_TEM_ASSET",
        "Vergi Öncesi Prim TL", "Net Prim Tutarı", "Vergisiz Prim",
        "Prim Tutarı (Vergi Hariç)", "Net Poliçe Primi", "VOP TL",
    ],
    "AS_DWH.GNL_KAYNAK_SISTEM": [
        "Üretim", "AS_DWH.GNL_KAYNAK_SISTEM",
        "Kaynak Sistem", "Veri Kaynağı", "Kaynak Uygulama",
        "Kaynak Platform", "Kaynak Sistem Adı", "Veri Üretim Sistemi",
    ],
    "AS_DWH.GNL_TARIH": [
        "Üretim", "AS_DWH.GNL_TARIH",
        "Ana Poliçe Tanzim Tarihi", "Poliçe Tanzim Tarihi", "Tanzim Tarihi",
        "Tan Tarih", "Tanzim Trh", "Tan Trh", "Bas Tarih", "Bas Trh",
        "Poliçe Düzenleme Tarihi", "Ana Poliçe Düzenleme Tarihi",
        "Poliçe Oluşturma Tarihi", "Poliçe Kayıt Tarihi",
        "Ek Tanzim Tarihi", "Ek Tarihi", "Ek Trh", "Ek Tnzm Tarihi", "Ek Tnzm Trh",
        "Zeyil Tanzim Tarihi", "Ek Düzenleme Tarihi", "Zeyil Düzenleme Tarihi",
        "Ek İşlem Tarihi", "Zeyil Kayıt Tarihi",
        "Ana Poliçe Başlangıç Tarihi", "Poliçe Başlangıç Tarihi",
        "Police Baslangic Tarihi", "Ana Plc Baslangic Tarihi", "Plc Bas Tarih",
        "Teminat Başlangıç Tarihi", "Sigorta Başlangıç Tarihi",
        "Poliçe Yürürlük Tarihi", "Teminat Başlangıcı",
        "Ek Başlangıç Tarihi", "Ek Bas Trh", "Ek Baslangic Trh", "Ek Bas Tarihi",
        "Zeyil Başlangıç Tarihi", "Ek Teminat Başlangıç Tarihi",
        "Zeyil Yürürlük Tarihi", "Ek Geçerlilik Başlangıç Tarihi", "Ek Teminat Yürürlük Tarihi",
        "Ana Poliçe Bitiş Tarihi", "Ana Poliçe Btitiş Tarihi", "Ana Pol Btis Tarihi", "Ana Pol BTS TRH",
        "Poliçe Bitiş Tarihi", "Poliçe Ana Bitiş Tarihi", "Bitiş Tarihi",
        "Teminat Bitiş Tarihi", "Sigorta Bitiş Tarihi", "Poliçe Sonlanma Tarihi", "Teminat Sonu Tarihi",
        "Ek Bitiş Tarihi", "Zeyil Bitiş Tarihi", "Ek Teminat Bitiş Tarihi",
        "Ek Geçerlilik Bitiş Tarihi", "Zeyil Geçerlilik Sonu", "Ek Süre Bitiş Tarihi",
    ],
    "AS_DWH.MUS_MUSTERI": [
        "Üretim", "AS_DWH.MUS_MUSTERI",
        "Müşteri No", "Müşteri", "Müşteri Numarası", "Müşteri ID", "Müşteri Tanımlama Numarası",
        "Müşteri Adı", "Sigortalı Adı", "Müşteri Unvanı", "Müşteri İsmi",
        "Müşteri Soyadı", "Soyadı", "Sigortalı Soyadı",
        "Müşteri Sayısı", "Toplam Müşteri Adedi", "Müşteri Adedi", "Müşteri Miktarı",
    ],
    "AS_DWH.POL_BRANS": [
        "Üretim", "AS_DWH.POL_BRANS",
        "Branş", "Sigorta Branşı", "Sigorta Türü", "Poliçe Branşı", "Branş Açıklama", "Hayır",
    ],
    "AS_DWH.POL_POLICE_DURUMU": [
        "Üretim", "AS_DWH.POL_POLICE_DURUMU",
        "Poliçe Durumu", "Poliçe Statüsü", "Poliçe Geçerlilik Durumu", "Poliçe Aktiflik Durumu",
    ],
    "AS_DWH.POL_POLICE_KIRILIM": [
        "Üretim", "AS_DWH.POL_POLICE_KIRILIM",
        "Branş", "Sigorta Branşı", "Sigorta Türü", "Poliçe Branşı", "Branş Açıklama", "Hayır",
        "TKZ Kodu", "Tarife Kodu", "Teminat Kodu", "Ürün Tarife Kodu", "Tarife Tanımlama Kodu",
        "TKZ Kodu Açıklaması", "TKZ Tanımı", "Tarife Kodu Açıklaması",
    ],
    "AS_DWH.POL_POLICE_OZET": [
        "Poliçe No", "Üretim", "AS_DWH.POL_POLICE_OZET",
        "Poliçe Numarası", "Pol No", "Pol Numarası", "Sigorta Poliçe Numarası",
        "Yenileme No", "Poliçe Yenileme Numarası", "Poliçe Yenileme Sayısı",
        "Tecdit No", "Tecdit Numarası",
        "Ek No", "Zeyil Numarası", "Poliçe Ek Numarası", "Zeyil Sıra Numarası",
        "Ek İşlem Numarası", "Ek Numarası", "Poliçe Ek No",
        "Poliçe Döviz Cinsi", "Poliçe Para Birimi", "Poliçe Kur Cinsi",
        "Poliçe Döviz Türü", "Poliçe Para Cinsi", "Poliçe Currency Bilgisi",
    ],
    "AS_DWH.POL_POLICE_REF_TANIM": [
        "Üretim", "AS_DWH.POL_POLICE_REF_TANIM",
        "Poliçe Adı", "Poliçe Açıklama", "Poliçe Kodu Açıklama", "Ürün Adı",
        "Poliçe Kodu Detay", "Poliçe Adı Detay",
        "Branş", "Sigorta Branşı", "Sigorta Türü", "Poliçe Branşı", "Branş Açıklama", "Hayır",
        "TKZ Kodu", "Tarife Kodu", "Teminat Kodu", "Ürün Tarife Kodu", "Tarife Tanımlama Kodu",
        "TKZ Kodu Açıklaması", "TKZ Tanımı", "Tarife Kodu Açıklaması",
    ],
    "AS_DWH.POL_POLICE_SATIS_KANALI": [
        "Üretim", "AS_DWH.POL_POLICE_SATIS_KANALI",
        "Satış Kanalı", "Satış Dağıtım Kanalı", "Poliçe Satış Kanalı",
        "Dağıtım Kanalı", "Satış Platformu",
    ],
    "AS_DWH.POL_URUN": [
        "Üretim", "AS_DWH.POL_URUN",
        "Ürün Adı", "Sigorta Ürün Adı", "Poliçe Ürün Adı", "Ürün Tanımı",
        "Sigorta Ürün İsmi", "Ürün Tanımlaması",
        "Ürün Kodu", "Sigorta Ürün Kodu", "Ürün Tanımlama Kodu",
    ],
}

# VOP routing rules (genuine cross-table business rules to keep)
_VOP_RULE_KOD = "Vergi Öncesi Prim TL ile çekerken AS_DWH.POL_POLICE_REF_TANIM tablosuna gitmelisin. Diğer Selectlerde AS_DWH.POL_POLICE_KOD tablosuna gidebilirsin."
_VOP_RULE_AS400 = "Vergi Öncesi Prim TL ile çekerken AS_DWH.POL_POLICE_REF_TANIM tablosuna gitmelisin. Diğer Selectlerde AGGR.AS400_YENI_URUN tablosuna gidebilirsin."
_VOP_RULE_BRANS = "Vergi Öncesi Prim TL ile çekerken AS_DWH.POL_POLICE_REF_TANIM tablosuna gitmelisin. Diğer Selectlerde AS_DWH.POL_BRANS tablosuna gidebilirsin."
_VOP_RULE_TKZ = "Vergi Öncesi Prim TL ile çekerken AS_DWH.POL_POLICE_REF_TANIM tablosuna gitmelisin. Diğer Selectlerde AS_DWH.POL_TKZ tablosuna gidebilirsin."

CLEAN_BUSINESS_NOTES: dict[str, list[str] | None] = {
    # None = remove business_notes key entirely
    "AGGR.AS400_YENI_URUN": [_VOP_RULE_AS400],
    "AS_DWH.ACE_ACENTE": None,
    "AS_DWH.ACE_ACENTE_ADRES": None,
    "AS_DWH.ACE_SATIS_MUDURLUGU": None,
    "AS_DWH.FACT_POL_POLICE_EK": None,
    "AS_DWH.FACT_POL_POLICE_TEM_ASSET": [_VOP_RULE_KOD, _VOP_RULE_AS400, _VOP_RULE_BRANS, _VOP_RULE_TKZ],
    "AS_DWH.GNL_KAYNAK_SISTEM": None,
    "AS_DWH.GNL_TARIH": None,
    "AS_DWH.MUS_MUSTERI": None,
    "AS_DWH.MUS_MUSTERI_CINSIYET": None,
    "AS_DWH.POLICE_SAYISI": None,
    "AS_DWH.POL_BRANS": [_VOP_RULE_BRANS],
    "AS_DWH.POL_POLICE_DURUMU": None,
    "AS_DWH.POL_POLICE_KIRILIM": [_VOP_RULE_BRANS, _VOP_RULE_TKZ],
    "AS_DWH.POL_POLICE_KOD": [_VOP_RULE_KOD],
    "AS_DWH.POL_POLICE_OZET": [_VOP_RULE_KOD, _VOP_RULE_AS400, _VOP_RULE_BRANS, _VOP_RULE_TKZ],
    "AS_DWH.POL_POLICE_REF_TANIM": [_VOP_RULE_KOD, _VOP_RULE_AS400, _VOP_RULE_BRANS, _VOP_RULE_TKZ],
    "AS_DWH.POL_POLICE_SATIS_KANALI": None,
    "AS_DWH.POL_TKZ": [_VOP_RULE_TKZ],
    "AS_DWH.POL_URUN": None,
}


def clean_document(doc: dict) -> dict:
    table_id = doc.get("id", "").strip()
    tm = doc.get("table_metadata", {})
    if not isinstance(tm, dict):
        return doc

    cleaned_tm: dict = {}

    # description
    desc = CLEAN_DESCRIPTION.get(table_id, tm.get("description", ""))
    if desc:
        cleaned_tm["description"] = desc

    # grain
    grain = CLEAN_GRAIN.get(table_id, tm.get("grain", ""))
    if grain:
        cleaned_tm["grain"] = grain

    # keywords
    kw = CLEAN_KEYWORDS.get(table_id, tm.get("keywords"))
    if isinstance(kw, list) and kw:
        cleaned_tm["keywords"] = kw

    # performance_rules — keep as-is
    pr = tm.get("performance_rules")
    if isinstance(pr, list) and pr:
        cleaned_tm["performance_rules"] = pr

    # relationships — keep as-is
    rels = tm.get("relationships")
    if isinstance(rels, list) and rels:
        cleaned_tm["relationships"] = rels

    # business_notes — apply cleanup
    if table_id in CLEAN_BUSINESS_NOTES:
        notes = CLEAN_BUSINESS_NOTES[table_id]
        if notes:  # non-empty list → keep
            cleaned_tm["business_notes"] = notes
        # None or empty → omit key entirely
    else:
        # table not in rules → keep only if non-empty
        bn = tm.get("business_notes")
        if isinstance(bn, list) and bn:
            cleaned_tm["business_notes"] = bn

    return {
        "id": doc.get("id", ""),
        "schema": doc.get("schema", ""),
        "name": doc.get("name", ""),
        "table_metadata": cleaned_tm,
    }


def main() -> None:
    print(f"Source: {SRC}")
    payload = json.loads(SRC.read_text(encoding="utf-8"))
    documents = payload.get("documents", [])

    cleaned = [clean_document(d) for d in documents]

    # Stats
    orig_notes = sum(
        len(d.get("table_metadata", {}).get("business_notes", []))
        for d in documents
    )
    new_notes = sum(
        len(d.get("table_metadata", {}).get("business_notes", []))
        for d in cleaned
    )
    desc_changes = sum(
        1 for d, c in zip(documents, cleaned)
        if d.get("table_metadata", {}).get("description") != c["table_metadata"].get("description")
    )
    grain_changes = sum(
        1 for d, c in zip(documents, cleaned)
        if d.get("table_metadata", {}).get("grain") != c["table_metadata"].get("grain")
    )

    result = {"documents": cleaned}

    # Write in-place
    SRC.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    orig_size = len(json.dumps(payload, ensure_ascii=False, indent=2))
    new_size = len(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"Tables: {len(cleaned)}")
    print(f"Description changes: {desc_changes}")
    print(f"Grain changes: {grain_changes}")
    print(f"Business notes: {orig_notes} -> {new_notes}")
    print(f"Size: {orig_size:,} -> {new_size:,} chars ({new_size/orig_size:.0%})")
    print("Done. File written in-place.")


if __name__ == "__main__":
    main()
