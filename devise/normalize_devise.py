"""
devise/normalize_devise.py
===========================
Normalisation du champ Devise.
Toute la donnée de référence est chargée depuis devise_referentiel.json.
Aucune constante métier n'est définie dans ce fichier.

COLONNES AJOUTÉES :
  Devise_clean       — valeur nettoyée (sans espaces, sans .0, majuscules)
  Devise_Normalisée  — code ISO 4217 alpha-3 / 'NA' / 'OUTLIER'
  Devise_method      — 'MAP' / 'NUM' / 'ALIAS' / 'STRIP' / 'NA' / 'OUTLIER' / 'WARM'
  Devise_check       — True si OUTLIER

RÈGLE NA :
  Devise == 'NA'  ET  Ref == 'NA'   → 'NA'
  Devise == 'NA'  ET  Ref != 'NA'   → OUTLIER
  Devise vide / null                → OUTLIER
  Valeur non identifiée             → OUTLIER

WARM-START :
  Si activé, lookup sur la valeur brute dans validated_classif.json avant la cascade.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from shared.base_pipeline import apply_na_rule


_RE_FLOAT_SUF = re.compile(r"\.0+$")
_RE_SPACES    = re.compile(r"\s+")
_RE_SYMBOLS   = re.compile(r"[^A-Za-z0-9]")
_RE_3DIGITS   = re.compile(r"^\d{3}$")


@dataclass
class DeviseReferentiel:
    version: str
    valid:   set  = field(default_factory=set)
    num_map: dict = field(default_factory=dict)
    aliases: dict = field(default_factory=dict)
    noise:   set  = field(default_factory=set)


def load_devise_referentiel(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return DeviseReferentiel(
        version = data.get("version", "unknown"),
        valid   = set(data.get("valid_iso4217", [])),
        num_map = data.get("num_to_iso", {}),
        aliases = data.get("aliases", {}),
        noise   = set(data.get("known_noise", [])),
    )


def load_warm_start_devise() -> dict:
    """Cache validé : clé = valeur brute rstrip, valeur = code ISO 4217."""
    path = Path(__file__).parent / "referentiel" / "validated_classif_devise.json"
    if not path.exists():
        return {}
    return json.load(open(path, encoding="utf-8")).get("classif", {})


def clean_devise(raw: str) -> str:
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    s = _RE_FLOAT_SUF.sub("", s)
    s = _RE_SPACES.sub("", s)
    return s.upper().strip()


def _strip_description(s):
    return s[:s.index("(")].strip() if "(" in s else s


def _alphanum_only(s):
    return _RE_SYMBOLS.sub("", s)


def _resolve_devise(raw_value, ref):
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None, None
    cleaned = clean_devise(str(raw_value))
    if not cleaned:
        return None, None
    if cleaned in ref.noise:
        return "OUTLIER", "OUTLIER"
    if cleaned in ref.valid:
        return cleaned, "MAP"
    if _RE_3DIGITS.match(cleaned) and cleaned in ref.num_map:
        return ref.num_map[cleaned], "NUM"
    if cleaned in ref.aliases:
        return ref.aliases[cleaned], "ALIAS"
    stripped = _strip_description(cleaned)
    if stripped != cleaned:
        if stripped in ref.valid:   return stripped, "STRIP"
        if stripped in ref.aliases: return ref.aliases[stripped], "STRIP"
    anum = _alphanum_only(cleaned)
    if anum in ref.valid:   return anum, "STRIP"
    if anum in ref.aliases: return ref.aliases[anum], "STRIP"
    if len(anum) > 3:
        prefix = anum[:3]
        if prefix in ref.valid:   return prefix, "STRIP"
        if prefix in ref.aliases: return ref.aliases[prefix], "STRIP"
    return "OUTLIER", "OUTLIER"


def treating_devise(
    df:         pd.DataFrame,
    devise_col: str              = "Devise",
    ref_col:    str              = "ReferenceTransaction",
    ref:        DeviseReferentiel = None,
    warm_start: bool             = False,
) -> pd.DataFrame:
    """
    Normalise la colonne Devise.

    warm_start : si True, résout d'abord via validated_classif.json (clé brute rstrip)
                 avant la cascade normale.
    """
    df = df.copy()

    if ref is None:
        default = Path(__file__).parent / "referentiel" / "devise_referentiel.json"
        ref = load_devise_referentiel(default)

    ws_cache: dict = {}
    if warm_start:
        ws_cache = load_warm_start_devise()
        #print(f"  Warm-start Devise : {len(ws_cache)} entrées chargées")

    # 1. Nettoyage sur valeurs uniques
    unique_vals = df[devise_col].dropna().unique()
    clean_map   = {v: clean_devise(str(v)) for v in unique_vals}
    df["Devise_clean"] = df[devise_col].map(clean_map).fillna("")

    # 2. Résolution — warm-start en premier (clé brute rstrip), cascade si absent
    iso_map: dict = {}
    for v in unique_vals:
        if warm_start:
            # Essai 1 : clé exacte (valeur brute telle quelle)
            if str(v) in ws_cache:
                iso_map[v] = (ws_cache[str(v)], "WARM")
                continue
            # Essai 2 : rstrip (gère les variantes d'espaces absentes du cache)
            key_rs = str(v).rstrip()
            if key_rs in ws_cache:
                iso_map[v] = (ws_cache[key_rs], "WARM")
                continue
        iso_map[v] = _resolve_devise(str(v), ref)

    df["Devise_Normalisée"] = df[devise_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["Devise_method"]     = df[devise_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # Flag warm-start : enregistré AVANT apply_na_rule qui peut écraser la méthode
    df["_ws_hit"] = df["Devise_method"] == "WARM"

    # 3. Règle NA — toujours en dernier, peu importe warm-start
    if ref_col in df.columns:
        fixed = df.apply(
            lambda row: apply_na_rule(
                row, devise_col, ref_col, "Devise_Normalisée", "Devise_method"
            ),
            axis=1,
            result_type="expand",
        )
        df["Devise_Normalisée"] = fixed[0]
        df["Devise_method"]     = fixed[1]

    # 4. Flag
    df["Devise_check"] = df["Devise_Normalisée"] == "OUTLIER"
    return df