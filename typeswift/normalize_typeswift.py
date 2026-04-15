"""
typeswift/normalize_typeswift.py
=================================
Normalisation du champ TypeSwift.
Référentiel chargé depuis typeswift_referentiel.json — aucune constante inline.

Les codes valides sont séparés par flux (FS / FE) — validés le 30/03/2026.
Le flux est transmis via le paramètre `flux` de treating_typeswift(),
lui-même lu depuis cfg["flux_type"] dans le YAML de l'API.

COLONNES AJOUTÉES :
  TypeSwift_clean   — clé de lookup (sans espaces, majuscules)
  TypeSwift_norm    — code canonique avec espaces / 'OUTLIER'
  TypeSwift_method  — 'MAP' / 'PREFIX' / 'OUTLIER'
  TypeSwift_check   — True si OUTLIER

RÈGLE NA : identique à tous les champs → apply_na_rule() de shared/base_pipeline.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import pandas as pd

from shared.base_pipeline import apply_na_rule


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════════════════════

def load_typeswift_referentiel(path: str | Path) -> dict:
    """
    Charge typeswift_referentiel.json.

    Construit pour chaque flux un dict de lookup sans espaces → forme canonique :
        ex: {"MT103": "MT 103", "MT103+": "MT 103 +", "PACS.008": "pacs.008"}

    Returns:
        dict avec clés :
            lookup_fs   (dict) — clé sans espaces upper → code canonique FS
            lookup_fe   (dict) — clé sans espaces upper → code canonique FE
            known_noise (set)  — valeurs bruit
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    def _build_lookup(codes: list) -> dict:
        lookup = {}
        for code in codes:
            key = re.sub(r"\s+", "", code).upper()
            lookup[key] = code
        return lookup

    return {
        "lookup_fs":   _build_lookup(data.get("valid_fs", [])),
        "lookup_fe":   _build_lookup(data.get("valid_fe", [])),
        "known_noise": set(data.get("known_noise", [])),
    }



def load_warm_start_typeswift(flux: str = "FE") -> dict:
    """Charge validated_classif_{flux}.json. Retourne {brut: label} ou {}."""
    path = Path(__file__).parent / "referentiel" / "validated_classif_typeswift_fe.json"
    if not path.exists():
        return {}
    return json.load(open(path, encoding="utf-8")).get("classif", {})


def load_warm_start_typeswift(flux: str = "FS") -> dict:
    """Cache warm-start TypeSwift. Cle = valeur brute exacte. NA exclu."""
    path = Path(__file__).parent / "referentiel" / "validated_classif_typeswift_fs.json"
    if not path.exists():
        return {}
    return json.load(open(path, encoding="utf-8")).get("classif", {})


# ══════════════════════════════════════════════════════════════════════════════
# REGEX
# ══════════════════════════════════════════════════════════════════════════════

_RE_SPACES = re.compile(r"\s+")
_RE_AMOUNT = re.compile(r"[\d,\.]{5,}|,")


# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE
# ══════════════════════════════════════════════════════════════════════════════

def clean_typeswift(raw: str) -> str:
    """Produit la clé de lookup : supprime espaces, majuscules. ex: 'MT 103 +' → 'MT103+'"""
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    return _RE_SPACES.sub("", s).upper()


# ══════════════════════════════════════════════════════════════════════════════
# RÉSOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_typeswift(
    raw_value: str,
    ref:       dict,
    lookup:    dict,
) -> tuple[Optional[str], Optional[str]]:
    """
    Résout une valeur TypeSwift selon le lookup du flux concerné.

    Cascade :
        A. Vide / NaN              → (None, None)
        B. Bruit connu / montant   → (OUTLIER, OUTLIER)
        C. Code dans lookup flux   → (forme canonique, MAP)
        D. Numéro seul 3 chiffres  → cherche MT{num} dans le lookup  (PREFIX)
        E. Non identifié           → (OUTLIER, OUTLIER)
    """
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None, None

    key = clean_typeswift(str(raw_value))
    if not key:
        return None, None

    # B. Bruit
    if key in ref["known_noise"] or _RE_AMOUNT.search(key):
        return "OUTLIER", "OUTLIER"

    # C. Lookup direct (clés déjà en upper sans espaces)
    if key in lookup:
        return lookup[key], "MAP"

    # D. Numéro seul → MT prefix
    if re.match(r"^\d{3}$", key):
        mt_key = f"MT{key}"
        if mt_key in lookup:
            return lookup[mt_key], "PREFIX"

    return "OUTLIER", "OUTLIER"


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def treating_typeswift(
    df:         pd.DataFrame,
    swift_col:  str  = "TypeSwfit",
    ref_col:    str  = "ReferenceTransaction",
    flux:       str  = "FS",
    ref:        dict = None,
    warm_start: bool = False,
) -> pd.DataFrame:
    """
    Normalise la colonne TypeSwift selon le flux (FS ou FE).
    Traitement sur valeurs uniques puis merge vectorisé.

    Args:
        df        : DataFrame source
        swift_col : colonne TypeSwift brute (défaut: TypeSwfit — typo BCM)
        ref_col   : colonne ReferenceTransaction
        flux      : "FS" (flux sortants) ou "FE" (flux entrants)
                    Lu depuis cfg["flux_type"] dans le YAML de l'API.
        ref       : référentiel chargé via load_typeswift_referentiel()
                    Si None → chargé automatiquement depuis typeswift_referentiel.json

    Ajoute :
        TypeSwift_clean   — clé de lookup (sans espaces, majuscules)
        TypeSwift_norm    — code canonique avec espaces / 'OUTLIER'
        TypeSwift_method  — MAP / PREFIX / OUTLIER
        TypeSwift_check   — True si OUTLIER
    """
    df = df.copy()

    if ref is None:
        default = Path(__file__).parent / "referentiel" / "typeswift_referentiel.json"
        ref = load_typeswift_referentiel(default)

    # Lookup selon le flux — transmis par le YAML via pipeline
    lookup = ref["lookup_fs"] if flux.upper() == "FS" else ref["lookup_fe"]

    # ── Warm-start ────────────────────────────────────────────────────────────
    ws_cache: dict = {}
    if warm_start:
        ws_cache = load_warm_start_typeswift(flux)
        print(f"  Warm-start TypeSwift {flux} : {len(ws_cache)} modalités connues")

    ws_cache: dict = {}
    if warm_start:
        ws_cache = load_warm_start_typeswift(flux)
        print(f"  Warm-start TypeSwift {flux} : {len(ws_cache)} entrees chargees")

    # 1. Nettoyage sur valeurs uniques
    unique_vals = df[swift_col].dropna().unique()
    clean_map   = {v: clean_typeswift(str(v)) for v in unique_vals}
    df["TypeSwift_clean"] = df[swift_col].map(clean_map).fillna("")

    # 2. Résolution sur valeurs uniques
    iso_map: dict = {}
    for v in unique_vals:
        if warm_start:
            if str(v) in ws_cache:
                iso_map[v] = (ws_cache[str(v)], "WARM"); continue
            key_rs = str(v).rstrip()
            if key_rs in ws_cache:
                iso_map[v] = (ws_cache[key_rs], "WARM"); continue
        iso_map[v] = _resolve_typeswift(str(v), ref, lookup)
    df["TypeSwift_Normalisé"]   = df[swift_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["TypeSwift_method"] = df[swift_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # 3. Warm-start sur valeur brute
    if warm_start and ws_cache:
        def _apply_ws_ts(row):
            brut = str(row.get(swift_col, "")).strip()
            if brut in ws_cache:
                return ws_cache[brut], "WARM"
            return row["TypeSwift_Normalisé"], row["TypeSwift_method"]
        ws_result = df.apply(_apply_ws_ts, axis=1, result_type="expand")
        df["TypeSwift_Normalisé"] = ws_result[0]
        df["TypeSwift_method"]     = ws_result[1]

    df["_ws_hit"] = df[swift_col].map(lambda v: iso_map.get(v,(None,None))[1] == "WARM")

    # 4. Règle NA — fonction commune shared/base_pipeline.py
    if ref_col in df.columns:
        fixed = df.apply(
            lambda row: apply_na_rule(row, swift_col, ref_col,
                                      "TypeSwift_Normalisé", "TypeSwift_method"),
            axis=1,
            result_type="expand",
        )
        df["TypeSwift_Normalisé"]   = fixed[0]
        df["TypeSwift_method"] = fixed[1]

    # 4. Flag
    df["TypeSwift_check"] = df["TypeSwift_Normalisé"] == "OUTLIER"
    return df