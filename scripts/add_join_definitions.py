"""Add comprehensive join_definitions to table_metadata_uretim.json.

Extracted from actual workbook SQL queries (VOP_TL path + BRUT_PRIM_TL path).
Run once:  py scripts/add_join_definitions.py
"""

import json
from pathlib import Path

FILE = Path("metadata/agents/table_metadata_uretim.json")

JOIN_DEFS: dict[str, list[dict]] = {
    "AGGR.AS400_YENI_URUN": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.NEW_FPOLK = AGGR.AS400_YENI_URUN.NEW_FPOLK",
        },
    ],
    "AS_DWH.ACE_ACENTE": [
        {
            "with_table": "AS_DWH.ACE_ACENTE_ADRES",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.ACE_ACENTE_ADRES.ACENTE_ID = AS_DWH.ACE_ACENTE.ACENTE_ID AND AS_DWH.ACE_ACENTE_ADRES.ALT_ACENTE_KODU = 0",
            "note": "ACE_ACENTE_ADRES uzerinde alias ALS_ACE_ADRES kullanilir. ALT_ACENTE_KODU = 0 filtresi ON clause icinde yer alir.",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "RIGHT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.ACENTE_ID = AS_DWH.ACE_ACENTE.ACENTE_ID",
            "note": "POL_POLICE_OZET ana tablodur; acente bilgisi olmayan policeler de gelmeli.",
        },
        {
            "with_table": "AS_DWH.ACE_SATIS_MUDURLUGU",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.ACE_ACENTE.BOLGE_MUDURLUGU_ID = AS_DWH.ACE_SATIS_MUDURLUGU.SATIS_MUDURLUGU_ID",
        },
    ],
    "AS_DWH.ACE_ACENTE_ADRES": [
        {
            "with_table": "AS_DWH.ACE_ACENTE",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.ACE_ACENTE_ADRES.ACENTE_ID = AS_DWH.ACE_ACENTE.ACENTE_ID AND AS_DWH.ACE_ACENTE_ADRES.ALT_ACENTE_KODU = 0",
            "alias": "ALS_ACE_ADRES",
            "note": "ALT_ACENTE_KODU = 0 filtresi ON clause icinde uygulanir.",
        },
    ],
    "AS_DWH.ACE_SATIS_MUDURLUGU": [
        {
            "with_table": "AS_DWH.ACE_ACENTE",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.ACE_ACENTE.BOLGE_MUDURLUGU_ID = AS_DWH.ACE_SATIS_MUDURLUGU.SATIS_MUDURLUGU_ID",
        },
    ],
    "AS_DWH.FACT_POL_POLICE_EK": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.FACT_POL_POLICE_EK.POLICE_ID AND AS_DWH.FACT_POL_POLICE_EK.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
            "note": "Composite join: her iki kolon birlikte ON clause icinde kullanilmalidir. BRUT_PRIM_TL sorgulari icin kullanilir.",
        },
    ],
    "AS_DWH.FACT_POL_POLICE_TEM_ASSET": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.FACT_POL_POLICE_TEM_ASSET.POLICE_ID = AS_DWH.POL_POLICE_OZET.POLICE_ID AND AS_DWH.FACT_POL_POLICE_TEM_ASSET.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
            "note": "Composite join: her iki kolon birlikte ON clause icinde kullanilmalidir. VOP_TL sorgulari icin kullanilir.",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_REF_TANIM",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.FACT_POL_POLICE_TEM_ASSET.REF_ID = AS_DWH.POL_POLICE_REF_TANIM.REF_ID",
            "alias": "ALS_FACT_POLICE_REF_TNM",
            "note": "VOP_TL sorgularinda kullanilir.",
        },
    ],
    "AS_DWH.GNL_KAYNAK_SISTEM": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.GNL_KAYNAK_SISTEM.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
        },
    ],
    "AS_DWH.GNL_TARIH": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_TANZIM_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_TANZIM_TARIH_ID = ALS_POL_ANA_TANZIM_TARIHI.TARIH_ID",
            "semantic": "Ana Police Tanzim Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_TANZIM_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.TANZIM_TARIH_ID = ALS_POL_EK_TANZIM_TARIHI.TARIH_ID",
            "semantic": "Ek Tanzim Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_BAS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_BASLANGIC_TARIH_ID = ALS_POL_ANA_BAS_TARIHI.TARIH_ID",
            "semantic": "Ana Police Baslangic Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_BAS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.BASLANGIC_TARIH_ID = ALS_POL_EK_BAS_TARIHI.TARIH_ID",
            "semantic": "Ek Baslangic Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_BITIS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_BITIS_TARIH_ID = ALS_POL_ANA_BITIS_TARIHI.TARIH_ID",
            "semantic": "Ana Police Bitis Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_BITIS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.BITIS_TARIH_ID = ALS_POL_EK_BITIS_TARIHI.TARIH_ID",
            "semantic": "Ek Bitis Tarihi",
        },
    ],
    "AS_DWH.MUS_MUSTERI": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.MUSTERI_ID = AS_DWH.MUS_MUSTERI.MUSTERI_ID",
        },
        {
            "with_table": "AS_DWH.MUS_MUSTERI_CINSIYET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.MUS_MUSTERI.MUSTERI_CINSIYET_ID = AS_DWH.MUS_MUSTERI_CINSIYET.MUSTERI_CINSIYET_ID",
        },
    ],
    "AS_DWH.MUS_MUSTERI_CINSIYET": [
        {
            "with_table": "AS_DWH.MUS_MUSTERI",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.MUS_MUSTERI.MUSTERI_CINSIYET_ID = AS_DWH.MUS_MUSTERI_CINSIYET.MUSTERI_CINSIYET_ID",
        },
    ],
    "AS_DWH.POLICE_SAYISI": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.POLICE_SAYISI.POLICE_ID",
        },
    ],
    "AS_DWH.POL_BRANS": [
        {
            "with_table": "AS_DWH.POL_POLICE_KIRILIM",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_KIRILIM.BRANS_ID = AS_DWH.POL_BRANS.BRANS_ID",
            "note": "KIRILIM path uzerinden erisim; VOP_TL sorgularinda POL_POLICE_REF_TANIM tercih edilir.",
        },
    ],
    "AS_DWH.POL_POLICE_DURUMU": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POLICE_DURUMU",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_DURUMU_KODU_AS400 = AS_DWH.POL_POLICE_DURUMU.POLICE_DURUMU_KODU",
        },
    ],
    "AS_DWH.POL_POLICE_KIRILIM": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.POL_POLICE_KIRILIM.POLICE_ID AND AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_KIRILIM.KAYNAK_SISTEM_ID",
            "note": "Composite join: her iki kolon birlikte kullanilmalidir.",
        },
        {
            "with_table": "AS_DWH.POL_BRANS",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_KIRILIM.BRANS_ID = AS_DWH.POL_BRANS.BRANS_ID",
        },
        {
            "with_table": "AS_DWH.POL_TKZ",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_KIRILIM.TKZ_ID = AS_DWH.POL_TKZ.TKZ_ID",
        },
    ],
    "AS_DWH.POL_POLICE_KOD": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_KOD_ID = AS_DWH.POL_POLICE_KOD.POLICE_KOD_ID",
            "note": "BRUT_PRIM_TL sorgularinda kullanilir; VOP_TL sorgularinda POL_POLICE_REF_TANIM tercih edilir.",
        },
    ],
    "AS_DWH.POL_POLICE_OZET": [
        {
            "with_table": "AS_DWH.ACE_ACENTE",
            "join_type": "RIGHT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.ACENTE_ID = AS_DWH.ACE_ACENTE.ACENTE_ID",
            "note": "RIGHT OUTER: ACE_ACENTE_ADRES INNER JOIN ACE_ACENTE bileseninin uzerine RIGHT yapilir; acente bilgisi olmayan policeler de gelir.",
        },
        {
            "with_table": "AS_DWH.MUS_MUSTERI",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.MUSTERI_ID = AS_DWH.MUS_MUSTERI.MUSTERI_ID",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_SATIS_KANALI",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_SATIS_KANALI.SATIS_KANALI_ID = AS_DWH.POL_POLICE_OZET.SATIS_KANALI_ID",
        },
        {
            "with_table": "AS_DWH.GNL_KAYNAK_SISTEM",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.GNL_KAYNAK_SISTEM.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_TANZIM_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_TANZIM_TARIH_ID = ALS_POL_ANA_TANZIM_TARIHI.TARIH_ID",
            "semantic": "Ana Police Tanzim Tarihi",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_TANZIM_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.TANZIM_TARIH_ID = ALS_POL_EK_TANZIM_TARIHI.TARIH_ID",
            "semantic": "Ek Tanzim Tarihi",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_BAS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_BASLANGIC_TARIH_ID = ALS_POL_ANA_BAS_TARIHI.TARIH_ID",
            "semantic": "Ana Police Baslangic Tarihi",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_BAS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.BASLANGIC_TARIH_ID = ALS_POL_EK_BAS_TARIHI.TARIH_ID",
            "semantic": "Ek Baslangic Tarihi",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POL_ANA_BITIS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.ANA_POLICE_BITIS_TARIH_ID = ALS_POL_ANA_BITIS_TARIHI.TARIH_ID",
            "semantic": "Ana Police Bitis Tarihi",
        },
        {
            "with_table": "AS_DWH.GNL_TARIH",
            "join_type": "INNER JOIN",
            "alias": "ALS_POL_EK_BITIS_TARIHI",
            "on": "AS_DWH.POL_POLICE_OZET.BITIS_TARIH_ID = ALS_POL_EK_BITIS_TARIHI.TARIH_ID",
            "semantic": "Ek Bitis Tarihi",
        },
        {
            "with_table": "AS_DWH.POL_URUN",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.URUN_KODU = AS_DWH.POL_URUN.URUN_KODU",
        },
        {
            "with_table": "AS_DWH.FACT_POL_POLICE_TEM_ASSET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.FACT_POL_POLICE_TEM_ASSET.POLICE_ID = AS_DWH.POL_POLICE_OZET.POLICE_ID AND AS_DWH.FACT_POL_POLICE_TEM_ASSET.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
            "note": "Composite join; VOP_TL sorgulari icin.",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_REF_TANIM",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_FACT_POLICE_REF_TNM",
            "on": "AS_DWH.FACT_POL_POLICE_TEM_ASSET.REF_ID = AS_DWH.POL_POLICE_REF_TANIM.REF_ID",
            "note": "TEM_ASSET uzerinden erisim; VOP_TL sorgulari icin.",
        },
        {
            "with_table": "AS_DWH.FACT_POL_POLICE_EK",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.FACT_POL_POLICE_EK.POLICE_ID AND AS_DWH.FACT_POL_POLICE_EK.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID",
            "note": "Composite join; BRUT_PRIM_TL sorgulari icin.",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_DURUMU",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_POLICE_DURUMU",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_DURUMU_KODU_AS400 = AS_DWH.POL_POLICE_DURUMU.POLICE_DURUMU_KODU",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_KOD",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_KOD_ID = AS_DWH.POL_POLICE_KOD.POLICE_KOD_ID",
            "note": "BRUT_PRIM_TL sorgulari icin.",
        },
        {
            "with_table": "AGGR.AS400_YENI_URUN",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.NEW_FPOLK = AGGR.AS400_YENI_URUN.NEW_FPOLK",
        },
        {
            "with_table": "AS_DWH.POL_POLICE_KIRILIM",
            "join_type": "INNER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.POL_POLICE_KIRILIM.POLICE_ID AND AS_DWH.POL_POLICE_OZET.KAYNAK_SISTEM_ID = AS_DWH.POL_POLICE_KIRILIM.KAYNAK_SISTEM_ID",
            "note": "Composite join; brans/TKZ kirilimi icin.",
        },
        {
            "with_table": "AS_DWH.POLICE_SAYISI",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.POLICE_ID = AS_DWH.POLICE_SAYISI.POLICE_ID",
        },
    ],
    "AS_DWH.POL_POLICE_REF_TANIM": [
        {
            "with_table": "AS_DWH.FACT_POL_POLICE_TEM_ASSET",
            "join_type": "LEFT OUTER JOIN",
            "alias": "ALS_FACT_POLICE_REF_TNM",
            "on": "AS_DWH.FACT_POL_POLICE_TEM_ASSET.REF_ID = AS_DWH.POL_POLICE_REF_TANIM.REF_ID",
            "note": "VOP_TL sorgularinda kullanilir; TEM_ASSET uzerinden erisim.",
        },
    ],
    "AS_DWH.POL_POLICE_SATIS_KANALI": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_SATIS_KANALI.SATIS_KANALI_ID = AS_DWH.POL_POLICE_OZET.SATIS_KANALI_ID",
        },
    ],
    "AS_DWH.POL_TKZ": [
        {
            "with_table": "AS_DWH.POL_POLICE_KIRILIM",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_KIRILIM.TKZ_ID = AS_DWH.POL_TKZ.TKZ_ID",
            "note": "KIRILIM path uzerinden erisim; VOP_TL sorgularinda POL_POLICE_REF_TANIM tercih edilir.",
        },
    ],
    "AS_DWH.POL_URUN": [
        {
            "with_table": "AS_DWH.POL_POLICE_OZET",
            "join_type": "LEFT OUTER JOIN",
            "on": "AS_DWH.POL_POLICE_OZET.URUN_KODU = AS_DWH.POL_URUN.URUN_KODU",
        },
    ],
}


def main() -> None:
    data = json.loads(FILE.read_text(encoding="utf-8"))
    docs = data["documents"]
    updated = 0
    for doc in docs:
        table_id = doc.get("id", "")
        defs = JOIN_DEFS.get(table_id)
        if defs is None:
            print(f"  SKIP  {table_id} (no join_definitions defined)")
            continue
        tm = doc.get("table_metadata")
        if not isinstance(tm, dict):
            print(f"  WARN  {table_id} has no table_metadata")
            continue
        tm["join_definitions"] = defs
        updated += 1
        print(f"  OK    {table_id} -> {len(defs)} join_definitions")

    FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"\nDone: {updated}/{len(docs)} tables updated in {FILE}")


if __name__ == "__main__":
    main()
