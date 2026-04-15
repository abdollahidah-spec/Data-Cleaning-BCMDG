"""
mode_reglement/normalize_mode_reglement.py
===========================================
Normalisation du champ ModeReglement.
Toute donnée métier chargée depuis mode_reglement_referentiel.json — aucune constante inline.

COLONNES AJOUTÉES :
  ModeReglement_clean   — valeur nettoyée (sans espaces, majuscules)
  ModeReglement_iso     — code normalisé / 'OUTLIER'
  ModeReglement_method  — 'MAP' / 'ALIAS' / 'OUTLIER'
  ModeReglement_check   — True si OUTLIER

RÈGLE NA / OUTLIER :
  ModeReglement n'a pas de cas NA légitime — tout vide/NA → OUTLIER.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

from shared.base_pipeline import apply_na_rule

from shared.base_pipeline import apply_na_rule


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════════════════════

def load_mode_referentiel(path: str | Path) -> dict:
    """
    Charge mode_reglement_referentiel.json.

    Structure attendue :
        version     : "1.0.0"
        valid_modes : ["CD", "RD", "TL"]
        aliases     : {"TR": "TL"}
        known_noise : ["NULL", "NONE", ...]
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {
        "valid_modes": set(data.get("valid_modes", [])),
        "aliases":     data.get("aliases", {}),
        "known_noise": set(data.get("known_noise", [])),
    }



def load_warm_start_mode(api_id: str = "E07_FS") -> dict:
    """
    Charge le cache warm-start ModeReglement.
    Cherche dans l'ordre :
      1. validated_classif_MR.json       ← fichier unique consolidé (tous APIs)
      2. validated_classif_{api_id}.json ← fichier par API (fallback)
    """
    base = Path(__file__).parent / "referentiel"
    # Essai 1 : fichier consolidé unique
    path = base / "validated_classif_MR.json"
    if path.exists():
        return json.load(open(path, encoding="utf-8")).get("classif", {})
    # Essai 2 : fichier par API
    path = base / f"validated_classif_{api_id}.json"
    if path.exists():
        return json.load(open(path, encoding="utf-8")).get("classif", {})
    return {}


# ══════════════════════════════════════════════════════════════════════════════
# REGEX
# ══════════════════════════════════════════════════════════════════════════════

_RE_SPACES = re.compile(r"\s+")


# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE
# ══════════════════════════════════════════════════════════════════════════════

@lru_cache(maxsize=4096)
def clean_mode_reglement(raw: str) -> str:
    """Trim + suppression espaces internes + MAJUSCULES."""
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    return _RE_SPACES.sub("", s).upper().strip()


# ══════════════════════════════════════════════════════════════════════════════
# RÉSOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_mode_reglement(
    raw_value: str,
    ref:       dict,
) -> tuple[Optional[str], Optional[str]]:
    """
    Cascade :
        A. Vide / NaN          → (None, None)
        B. Bruit connu         → (OUTLIER, OUTLIER)
        C. Code direct valide  → (CD, MAP)
        D. Alias connu         → (TL, ALIAS)
        E. Non identifié       → (OUTLIER, OUTLIER)
    """
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None, None

    cleaned = clean_mode_reglement(str(raw_value))
    if not cleaned:
        return None, None

    if cleaned in ref["known_noise"]:
        return "OUTLIER", "OUTLIER"

    if cleaned in ref["valid_modes"]:
        return cleaned, "MAP"

    if cleaned in ref["aliases"]:
        return ref["aliases"][cleaned], "ALIAS"

    return "OUTLIER", "OUTLIER"


# ══════════════════════════════════════════════════════════════════════════════
# RÈGLE NA / OUTLIER
# ══════════════════════════════════════════════════════════════════════════════

# _apply_na_rule_mode → remplacé par apply_na_rule() de shared/build_tables.py


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def treating_mode_reglement(
    df:         pd.DataFrame,
    mode_col:   str  = "ModeReglement",
    ref_col:    str  = "ReferenceTransaction",
    ref:        dict = None,
    warm_start: bool = False,
    api_id:     str  = "E07_FS",
) -> pd.DataFrame:
    """
    Normalise la colonne ModeReglement.
    Traitement sur valeurs uniques puis merge vectorisé — pas de df.apply ligne par ligne.

    Args:
        df       : DataFrame source
        mode_col : colonne ModeReglement brute
        ref_col  : colonne ReferenceTransaction
        ref      : référentiel chargé via load_mode_referentiel()
                   Si None → chargé automatiquement depuis mode_reglement_referentiel.json

    Ajoute :
        ModeReglement_clean   — valeur nettoyée
        ModeReglement_iso     — code normalisé / 'OUTLIER'
        ModeReglement_method  — MAP / ALIAS / OUTLIER
        ModeReglement_check   — True si OUTLIER
    """
    df = df.copy()

    if ref is None:
        default = Path(__file__).parent / "referentiel" / "mode_reglement_referentiel.json"
        ref = load_mode_referentiel(default)

    # ── Warm-start ────────────────────────────────────────────────────────────
    ws_cache: dict = {}
    ws_cache_upper: dict = {}
    if warm_start:
        ws_cache       = load_warm_start_mode(api_id)
        ws_cache_upper = {k.strip().upper(): v for k, v in ws_cache.items()}
        print(f"  Warm-start ModeReglement {api_id} : {len(ws_cache)} modalités connues")

    # 1. Nettoyage sur valeurs uniques
    unique_vals = df[mode_col].dropna().unique()
    clean_map   = {v: clean_mode_reglement(str(v)) for v in unique_vals}
    df["ModeReglement_clean"] = df[mode_col].map(clean_map).fillna("")

    # 2. Résolution — warm-start en premier (4 essais), cascade si absent
    iso_map: dict = {}
    for v in unique_vals:
        if warm_start:
            s = str(v)
            if s in ws_cache:                lbl = ws_cache[s];              iso_map[v] = (lbl, "WARM"); continue
            if s.strip() in ws_cache:        lbl = ws_cache[s.strip()];      iso_map[v] = (lbl, "WARM"); continue
            if s.rstrip() in ws_cache:       lbl = ws_cache[s.rstrip()];     iso_map[v] = (lbl, "WARM"); continue
            key_up = s.strip().upper()
            if key_up in ws_cache_upper:     lbl = ws_cache_upper[key_up];   iso_map[v] = (lbl, "WARM"); continue
        iso_map[v] = _resolve_mode_reglement(str(v), ref)
    df["ModeReglement_Normalisé"]    = df[mode_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["ModeReglement_method"] = df[mode_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # 3. Warm-start sur valeur brute
    if warm_start and ws_cache:
        def _apply_ws_mr(row):
            brut = str(row.get(mode_col, "")).strip()
            if brut in ws_cache:
                return ws_cache[brut], "WARM"
            return row["ModeReglement_Normalisé"], row["ModeReglement_method"]
        ws_result = df.apply(_apply_ws_mr, axis=1, result_type="expand")
        df["ModeReglement_Normalisé"] = ws_result[0]
        df["ModeReglement_method"]     = ws_result[1]

    df["_ws_hit"] = df[mode_col].map(lambda v: iso_map.get(v,(None,None))[1] == "WARM")

    # 4. Règle NA — même pattern que Devise (row-based)
    if ref_col in df.columns:
        fixed = df.apply(
            lambda row: apply_na_rule(row, mode_col, ref_col, "ModeReglement_Normalisé", "ModeReglement_method"),
            axis=1,
            result_type="expand",
        )
        df["ModeReglement_Normalisé"]    = fixed[0]
        df["ModeReglement_method"] = fixed[1]

    # 4. Flag
    df["ModeReglement_check"] = df["ModeReglement_Normalisé"] == "OUTLIER"
    return df