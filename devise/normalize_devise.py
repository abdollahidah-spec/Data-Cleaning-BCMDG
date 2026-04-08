"""
devise/normalize_devise.py
===========================
Normalisation du champ Devise.
Toute la donnée de référence est chargée depuis devise_referentiel.json.
Aucune constante métier n'est définie dans ce fichier.

COLONNES AJOUTÉES :
  Devise_clean   — valeur nettoyée (sans espaces, sans .0, majuscules)
  Devise_iso     — code ISO 4217 alpha-3 / 'NA' / 'OUTLIER'
  Devise_method  — 'MAP' / 'NUM' / 'ALIAS' / 'STRIP' / 'NA' / 'OUTLIER'
  Devise_check   — True si OUTLIER

RÈGLE NA / OUTLIER :
  Devise vide/null/NA  +  Ref vide/null/NA/string  →  'NA'
  Devise vide/null/NA  +  Ref non vide             →  OUTLIER
  Valeur non identifiée                            →  OUTLIER
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import pandas as pd

from shared.base_pipeline import apply_na_rule

# ══════════════════════════════════════════════════════════════════════════════
# REGEX (purement techniques — pas de données métier)
# ══════════════════════════════════════════════════════════════════════════════

_RE_FLOAT_SUF = re.compile(r"\.0+$")
_RE_SPACES    = re.compile(r"\s+")
_RE_SYMBOLS   = re.compile(r"[^A-Za-z0-9]")
_RE_3DIGITS   = re.compile(r"^\d{3}$")


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class DeviseReferentiel:
    version:   str
    valid:     set  = field(default_factory=set)   # codes ISO alpha-3 valides
    num_map:   dict = field(default_factory=dict)  # numérique → alpha-3
    aliases:   dict = field(default_factory=dict)  # alias → alpha-3
    noise:     set  = field(default_factory=set)   # bruit connu → OUTLIER direct


def load_devise_referentiel(path: str | Path) -> DeviseReferentiel:
    """
    Charge devise_referentiel.json et retourne un DeviseReferentiel.

    Structure attendue du JSON :
        version       : "1.0.0"
        valid_iso4217 : ["USD", "EUR", ...]
        num_to_iso    : {"840": "USD", "978": "EUR", ...}
        aliases       : {"CFA": "XOF", "MRO": "MRU", ...}
        known_noise   : ["STRING", "CHBANK", ...]
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return DeviseReferentiel(
        version = data.get("version", "unknown"),
        valid   = set(data.get("valid_iso4217", [])),
        num_map = data.get("num_to_iso", {}),
        aliases = data.get("aliases", {}),
        noise   = set(data.get("known_noise", [])),
    )


# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE
# ══════════════════════════════════════════════════════════════════════════════

def clean_devise(raw: str) -> str:
    """
    Nettoie une valeur brute :
      1. Trim
      2. Supprime suffixe .0 / .00  (840.0 → 840)
      3. Supprime les espaces internes
      4. MAJUSCULES
    """
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    s = _RE_FLOAT_SUF.sub("", s)
    s = _RE_SPACES.sub("", s)
    return s.upper().strip()


def _strip_description(s: str) -> str:
    """Extrait le code avant une description entre parenthèses. CAD (CANADIAN DOLLAR) → CAD"""
    return s[:s.index("(")].strip() if "(" in s else s


def _alphanum_only(s: str) -> str:
    """Retire tout caractère non alphanumérique."""
    return _RE_SYMBOLS.sub("", s)


# ══════════════════════════════════════════════════════════════════════════════
# RÉSOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_devise(raw_value: str, ref: DeviseReferentiel) -> tuple[Optional[str], Optional[str]]:
    """
    Résout une valeur brute en code ISO 4217 alpha-3.
    Toutes les données métier (codes valides, mappings, aliases) viennent de ref.

    Cascade :
        A. Vide / NaN              → (None, None)
        B. Bruit connu             → (OUTLIER, OUTLIER)
        C. Code ISO alpha-3 direct → (USD, MAP)
        D. Code numérique          → (USD, NUM)   via ref.num_map
        E. Alias connu             → (XOF, ALIAS) via ref.aliases
        F. Strip description ()    → (CAD, STRIP)
        G. Nettoyage alphanum      → (USD, STRIP) ex: USDFf → USD
        H. Non identifié           → (OUTLIER, OUTLIER)
    """
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None, None

    cleaned = clean_devise(str(raw_value))
    if not cleaned:
        return None, None

    # B. Bruit connu
    if cleaned in ref.noise:
        return "OUTLIER", "OUTLIER"

    # C. Code ISO direct
    if cleaned in ref.valid:
        return cleaned, "MAP"

    # D. Code numérique (ex: 978 → EUR)
    if _RE_3DIGITS.match(cleaned) and cleaned in ref.num_map:
        return ref.num_map[cleaned], "NUM"

    # E. Alias (ex: CFA → XOF, MRO → MRU)
    if cleaned in ref.aliases:
        return ref.aliases[cleaned], "ALIAS"

    # F. Strip description (ex: CAD (CANADIAN DOLLAR) → CAD)
    stripped = _strip_description(cleaned)
    if stripped != cleaned:
        if stripped in ref.valid:   return stripped, "STRIP"
        if stripped in ref.aliases: return ref.aliases[stripped], "STRIP"

    # G. Nettoyage alphanum (ex: USDFf → USDFF → USD)
    anum = _alphanum_only(cleaned)
    if anum in ref.valid:   return anum, "STRIP"
    if anum in ref.aliases: return ref.aliases[anum], "STRIP"
    if len(anum) > 3:
        prefix = anum[:3]
        if prefix in ref.valid:   return prefix, "STRIP"
        if prefix in ref.aliases: return ref.aliases[prefix], "STRIP"

    # H. Non identifié
    return "OUTLIER", "OUTLIER"


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def treating_devise(
    df:         pd.DataFrame,
    devise_col: str = "Devise",
    ref_col:    str = "ReferenceTransaction",
    ref:        DeviseReferentiel = None,
) -> pd.DataFrame:
    """
    Normalise la colonne Devise. Traitement sur valeurs uniques puis merge (performant).

    Args:
        df         : DataFrame source
        devise_col : colonne Devise brute
        ref_col    : colonne ReferenceTransaction (ou NumCredoc pour E09)
        ref        : référentiel chargé via load_devise_referentiel()
                     Si None → chargé automatiquement depuis devise_referentiel.json

    Ajoute :
        Devise_clean   — valeur nettoyée
        Devise_iso     — code ISO 4217 / 'NA' / 'OUTLIER'
        Devise_method  — méthode : MAP / NUM / ALIAS / STRIP / NA / OUTLIER
        Devise_check   — True si OUTLIER
    """
    df = df.copy()

    # Référentiel par défaut
    if ref is None:
        default = Path(__file__).parent / "referentiel" / "devise_referentiel.json"
        ref = load_devise_referentiel(default)

    # 1. Nettoyage sur valeurs uniques → merge
    unique_vals = df[devise_col].dropna().unique()
    clean_map   = {v: clean_devise(str(v)) for v in unique_vals}
    df["Devise_clean"] = df[devise_col].map(clean_map).fillna("")

    # 2. Résolution sur valeurs uniques → merge (évite df.apply ligne par ligne)
    iso_map = {v: _resolve_devise(str(v), ref) for v in unique_vals}
    df["Devise_iso"]    = df[devise_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["Devise_method"] = df[devise_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # 3. Règle NA — appliquée ligne par ligne (signature row-based comme dans les notebooks)
    if ref_col in df.columns:
        fixed = df.apply(
            lambda row: apply_na_rule(row, devise_col, ref_col, "Devise_iso", "Devise_method"),
            axis=1,
            result_type="expand",
        )
        df["Devise_iso"]    = fixed[0]
        df["Devise_method"] = fixed[1]

    # 4. Flag
    df["Devise_check"] = df["Devise_iso"] == "OUTLIER"
    return df