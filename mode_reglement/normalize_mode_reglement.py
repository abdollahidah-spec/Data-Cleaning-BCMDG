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

"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Optional
import pandas as pd

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
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def treating_mode_reglement(
    df:       pd.DataFrame,
    mode_col: str  = "ModeReglement",
    ref_col:  str  = "ReferenceTransaction",
    ref:      dict = None,
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

    # 1. Nettoyage sur valeurs uniques
    unique_vals = df[mode_col].dropna().unique()
    clean_map   = {v: clean_mode_reglement(str(v)) for v in unique_vals}
    df["ModeReglement_clean"] = df[mode_col].map(clean_map).fillna("")

    # 2. Résolution sur valeurs uniques
    iso_map = {v: _resolve_mode_reglement(str(v), ref) for v in unique_vals}
    df["ModeReglement_iso"]    = df[mode_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["ModeReglement_method"] = df[mode_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # 3. Règle NA — même pattern que Devise (row-based)
    if ref_col in df.columns:
        fixed = df.apply(
            lambda row: apply_na_rule(row, mode_col, ref_col, "ModeReglement_iso", "ModeReglement_method"),
            axis=1,
            result_type="expand",
        )
        df["ModeReglement_iso"]    = fixed[0]
        df["ModeReglement_method"] = fixed[1]

    # 4. Flag
    df["ModeReglement_check"] = df["ModeReglement_iso"] == "OUTLIER"
    return df