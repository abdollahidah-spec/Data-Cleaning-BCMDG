"""
nature_economique/normalize_nature_economique.py
==================================================
Normalisation du champ NatureEconomique.

Données métier séparées par flux :
  - Référentiel (catégories → labels)  : referentiel/referentiel_{FS|FE}.json
  - Few-shot LLM                        : referentiel/few_shot_{FS|FE}.json
  - Mapping direct, system_prompt,
    non_classe_direct                   : constantes dans ce fichier (FS et FE séparés)

COLONNES AJOUTÉES :
  NatEco_Clean     — valeur nettoyée
  NatEco_Categorie — catégorie normalisée / 'OUTLIER' / 'NA'
  NatEco_Label     — label détaillé / 'OUTLIER' / 'NA' / 'AUTRES'
  NatEco_Methode   — 'REGLE' / 'EMBED' / 'LLM'
  NatEco_Check     — 'OK' / 'CHECK'

PIPELINE (4 étapes) :
  0. Règles spéciales + mapping direct  → REGLE
  1. Embedding sémantique (mpnet)       → EMBED  (seuil adaptatif)
  2. LLM Qwen fallback via ollama_client→ LLM
  3. Rediffusion cache → toutes lignes

RÈGLE NA :
  ref == 'NA'  ET  nat == 'NA'  →  ('NA', 'NA')
  Tout autre vide / bruit       →  ('OUTLIER', 'OUTLIER')
  non_classe_direct             →  ('OUTLIER', 'OUTLIER')  — pas de catégorie NON CLASSE
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd

from shared.ollama_client import call_llm_nateco_batch


# ══════════════════════════════════════════════════════════════════════════════
# MAPPING DIRECT — différent pour FS et FE
# ══════════════════════════════════════════════════════════════════════════════

_MAPPING_DIRECT_FS: dict[str, Optional[str]] = {
    "RIZ": "PRODUITS ALIMENTAIRES", "LAIT": "PRODUITS ALIMENTAIRES",
    "PRODUIT LITERIAIRE": "PRODUITS ALIMENTAIRES", "MILK": "PRODUITS ALIMENTAIRES",
    "VIANDE": "PRODUITS ALIMENTAIRES", "HUILE": "PRODUITS ALIMENTAIRES",
    "MARGARINE": "PRODUITS ALIMENTAIRES", "FUEL": "PRODUITS PETROLIERS GAZ",
    "TISSU": "PRODUITS TEXTILES", "TISSUS": "PRODUITS TEXTILES",
    "TUSSUS": "PRODUITS TEXTILES", "VETEMENT": "PRODUITS TEXTILES",
    "VETEMNTS": "PRODUITS TEXTILES", "COTTON": "PRODUITS TEXTILES",
    "VETEMENTS": "PRODUITS TEXTILES",
    "ENGINS": "EQUIPEMENTS MACHINES", "VEHICLE": "AUTOMOBILES VEHICULES",
    "TRUCK": "AUTOMOBILES VEHICULES", "MOTO": "AUTOMOBILES VEHICULES",
    "EQUIPENTS": "EQUIPEMENTS MACHINES", "HONORAIRES": "SERVICE CONSEIL GESTION",
    "PRODUIT CERAMIQUE": "MATERIAUX CONSTRUCTION", "CERAMIC": "MATERIAUX CONSTRUCTION",
    "TRANSPORT": "FRAIS ANNEXES TRANSPORT",
    "TRANSPORTS AERIENS PASSAGERS": "BILLETS AVION",
    "IMPORTATIONS DES BIENS": "AUTRES BIENS", "IMPORTATION DE BIENS": "AUTRES BIENS",
    "FRAIS DE SCOLARITE": "EDUCATION FORMATION",
    "PRET AUTRES SECTEURS": "PRET AUTRES SECTEURS LMT",
    "COTISATIONS": "PENSIONS RETRAITES COTISATIONS",
    "EXPORTATION ET IMPORTATION DE BIENS": "AUTRES BIENS",
    "IMPPORTATION EXPORTATION DE BIEN": "AUTRES BIENS",
    "IMP DES BIENS": "AUTRES BIENS", "IMPORTATION DESZ BIENS": "AUTRES BIENS",
    "EXPORTATION ET IMPORTAION DE BIENS": "AUTRES BIENS",
    "FRET": "FRAIS ET TAXES", "FRAIS DE VOYAGE ET DE SEJOUR": "TOURISME SEJOUR",
    "SERVICE INFORMATION": "SERVICE TELECOM INFORMATIQUE",
    "AUTRES INTERMEDIAIRES COMCE DE": "AUTRES SERVICES",
    "AUTRES INTERMEDIAIRES COMCE PR": "AUTRES SERVICES",
    "SERVICES DHL ET CENTRE APPEL ET AUTRES MESSAGERIES": "SERVICES MESSAGERIES EXPRESS",
    "ACTIVITES ORGANIS POLITIQUES":  "ADMINISTRATIONS PUBLIQUES",
    "ACTIVS ORDRE PUBLIC SECURITE":  "SERVICES SECURITE",
    "ACTIVS ORGANIS PATRONALES CONS": "SERVICE CONSEIL GESTION",
    "ACTIVS ORGANIS PROFESSIONNELLE": "SERVICE CONSEIL GESTION",
    "ADMINIST MARCHES FINANCIERS":   "SERVICES BANCAIRES FINANCIERS",
    "SERVICES DHL ET POSTE ET AUTRES MESSAGERIES":        "SERVICES MESSAGERIES EXPRESS",
    "PECHE": "PECHES PRODUITS MARITIMES", "POISSONE": "PECHES PRODUITS MARITIMES",
    "EXPORTATION POISSON": "PECHES PRODUITS MARITIMES",
    "ACTIVITES JURIDIQUES": "ACTES JUDICIAIRES", "JUSTICE": "ACTES JUDICIAIRES",
    "ACTIVITES COMPTABLES": "SERVICES COMPTABLES",
    "VOYAGE TOURISTIQUE": "TOURISME SEJOUR",
    "HEBERGEMENT PELERINAGE OU OMRA": "TOURISME SEJOUR",
    "AFFRETEMENT PELERINAGE": "TOURISME SEJOUR",
    "FRAIS SCOLARITE": "EDUCATION FORMATION", "FRAIS SCOLAIRE": "EDUCATION FORMATION",
    "REGL LOYER": "LOYER", "REGLEMENT LOYERS": "LOYER", "LOYERS": "LOYER",
    "SALAIRE": "SALAIRES", "SALAIRES EMPLOYES": "SALAIRES",
    "SALAIRES ET APPOINTEMENTS": "SALAIRES", "ECONOMIE SUR SALAIRE": "SALAIRES",
    "ECONOMIE SUR REVENUS DES MAURITANIENS": "ECONOMIE REVENUS MAURITANIENS",
    "ECONOMIE SUR LES REVENUS DES ETRANGERS": "ECONOMIE REVENUS ETRANGERS",
    "SALAIRES EMPLOYES LIBYENNES": "ECONOMIE REVENUS ETRANGERS",
    "SALAIRES DES LIBYENNES": "ECONOMIE REVENUS ETRANGERS",
    "AIDE FAMILIALE": "AIDES FAMILIALES", "AIDE FAMILIALS": "AIDES FAMILIALES",
    "ALLOCATION": "AIDES FAMILIALES",
    "PENSION": "PENSIONS RETRAITES COTISATIONS",
    "PENSIONS ET RENTES": "PENSIONS RETRAITES COTISATIONS",
    "NIVELLEMENT DE FONDS": "AVANCE RETOUR DE FOND",
    "APPROVISIONNEMENT COMPTE": "AVANCE RETOUR DE FOND",
    "AMBASSADE": "AMBASSADES CONSULATS", "AMBASSADES": "AMBASSADES CONSULATS",
    "DONS POUR LES INVESTISSEMENTS": "DONS POUR INVESTISSEMENTS",
    "COSULTING": "SERVICE CONSEIL GESTION",
    "SERVICE INFO": "SERVICE TELECOM INFORMATIQUE",
    "TELECOMMUNICATION": "SERVICE TELECOM INFORMATIQUE",
    "SERVICES INFORMATIQUES": "SERVICE TELECOM INFORMATIQUE",
    "FEES": "FRAIS ET TAXES", "FRAIS AVOCAT": "FRAIS AVOCATS",
    "FRAIS AVOCATS": "FRAIS AVOCATS",
    "AUTRE": "AUTRES", "AUTES": "AUTRES", "AUTREQS": "AUTRES",
    "AUTREES": "AUTRES", "AITRES": "AUTRES", "AUTERS": "AUTRES",
    "REF AUTERS": "AUTRES", "AUTRS": "AUTRES",
    # Outliers directs
    "GB": None, "ESPAGNE": None, "USA": None, "MALI": None,
    "ANGOLA": None, "FRANCE": None, "CANADA": None, "INFORMELS": None,
    "B": None, "N": None, "FAUX PARTICULIERS": None,
    "PASSPORT ID": None, "STUDENT ID": None,
}

_MAPPING_DIRECT_FE: dict[str, Optional[str]] = {
    "FEES": "FRAIS ET TAXES", "FRAIS AVOCAT": "FRAIS AVOCATS",
    "FRAIS AVOCATS": "FRAIS AVOCATS",
    "ACTIVITES AGENCES PUBLICITE": "SERVICE COMMUNICATION",
    "CONSEIL REL PUBL COMMUNICATION": "SERVICE COMMUNICATION",
    "SERVICES INFORMATION": "SERVICE TELECOM INFORMATIQUE",
    "ACTIVITES JURIDIQUES": "ACTES JUDICIAIRES", "JUSTICE": "ACTES JUDICIAIRES",
    "ACTIVITES COMPTABLES": "SERVICES COMPTABLES",
    "PECHE": "PECHES PRODUITS MARITIMES", "POISSONS": "PECHES PRODUITS MARITIMES",
    "POISSONE": "PECHES PRODUITS MARITIMES",
    "EXPORTATION POISSON": "PECHES PRODUITS MARITIMES",
    "EXPORTATION DE PECHE": "PECHES PRODUITS MARITIMES",
    "EXPORTATION ET IMPORTATION DE BIENS": "AUTRES BIENS",
    "EXPORTATION ET IMPORTATION DES BIENS": "AUTRES BIENS",
    "IMP EXP DE BIEN": "AUTRES BIENS", "EXPORTATION BIEN": "AUTRES BIENS",
    "EXPORTATIONS": "AUTRES BIENS", "EXPORTATION": "AUTRES BIENS",
    "IMPORTATION": "AUTRES BIENS", "COMMERCE DIVERS": "AUTRES BIENS",
    "COMMERCE": "AUTRES BIENS", "COMMERCE ET AFFAIRES": "AUTRES BIENS",
    "TRANSPORT": "FRAIS ANNEXES TRANSPORT",
    "FRAIS DE TRANSPORT": "FRAIS ANNEXES TRANSPORT",
    "AUTRES AFFRETEMENTS": "FRAIS ANNEXES TRANSPORT",
    "AVIONS BATEAUX": "BILLETS AVION", "VENTE DE BILLETS D AVION": "BILLETS AVION",
    "FRAIS DE VOYAGE ET DE SEJOUR": "TOURISME SEJOUR",
    "VOYAGE TOURISTIQUE": "TOURISME SEJOUR",
    "HEBERGEMENT PELERINAGE OU OMRA": "TOURISME SEJOUR",
    "AFFRETEMENT PELERINAGE": "TOURISME SEJOUR",
    "FRAIS DE SCOLARITE": "EDUCATION FORMATION",
    "FRAIS SCOLARITE": "EDUCATION FORMATION", "FRAIS SCOLAIRE": "EDUCATION FORMATION",
    "REGL LOYER": "LOYER", "REGLEMENT LOYERS": "LOYER", "LOYERS": "LOYER",
    "SALAIRE": "SALAIRES", "SALAIRES EMPLOYES": "SALAIRES",
    "SALAIRES ET APPOINTEMENTS": "SALAIRES", "ECONOMIE SUR SALAIRE": "SALAIRES",
    "ECONOMIE SUR REVENUS DES MAURITANIENS": "ECONOMIE REVENUS MAURITANIENS",
    "ECONOMIE SUR LES REVENUS DES ETRANGERS": "ECONOMIE REVENUS ETRANGERS",
    "SALAIRES EMPLOYES LIBYENNES": "ECONOMIE REVENUS ETRANGERS",
    "SALAIRES DES LIBYENNES": "ECONOMIE REVENUS ETRANGERS",
    "AIDE FAMILIALE": "AIDES FAMILIALES", "AIDE FAMILIALS": "AIDES FAMILIALES",
    "ALLOCATION": "AIDES FAMILIALES",
    "PENSION": "PENSIONS RETRAITES COTISATION",
    "PENSIONS ET RENTES": "PENSIONS RETRAITES COTISATION",
    "NIVELLEMENT DE FONDS": "AVANCE RETOUR DE FOND",
    "APPROVISIONNEMENT COMPTE": "AVANCE RETOUR DE FOND",
    "AMBASSADE": "AMBASSADES CONSULATS", "AMBASSADES": "AMBASSADES CONSULATS",
    "DONS POUR LES INVESTISSEMENTS": "DONS POUR INVESTISSEMENTS",
    "HONORAIRES": "SERVICE CONSEIL GESTION", "COSULTING": "SERVICE CONSEIL GESTION",
    "SERVICE INFORMATION": "SERVICE TELECOM INFORMATIQUE",
    "SERVICE INFO": "SERVICE TELECOM INFORMATIQUE",
    "TELECOMMUNICATION": "SERVICE TELECOM INFORMATIQUE",
    "SERVICES INFORMATIQUES": "SERVICE TELECOM INFORMATIQUE",
    "AUTRE": "AUTRES", "AUTRS": "AUTRES", "AUTERS": "AUTRES",
    # Outliers directs
    "GB": None, "ESPAGNE": None, "USA": None, "MALI": None,
    "ANGOLA": None, "FRANCE": None, "CANADA": None,
    "INFORMELS": None, "B": None, "N": None, "FAUX PARTICULIERS": None,
}

# Modalités vagues → OUTLIER directement (pas de catégorie NON CLASSE)
_NON_CLASSE_DIRECT_FS: set[str] = {
    "TRANSFERTS", "IMMOBILIER", "INVESTISSEMENTS",
    "AFFAIRES ETRANGERES", "AFFAIRES NON CLASSEES",
    "VOYAGE D?AFFAIRES", "COMMISSIONS COURTAGES",
    "PARTICULIERS ET PROFETIONNEL", "VOYAGE D AFFAIRES",
}

_NON_CLASSE_DIRECT_FE: set[str] = {
    "TRANSFERTS", "IMMOBILIER", "INVESTISSEMENTS",
    "AFFAIRES ETRANGERES", "AFFAIRES NON CLASSEES",
    "COMMISSIONS COURTAGES", "PARTICULIERS ET PROFETIONNEL",
}

# System prompts — différents car FS répond OUTLIER, FE répondait NON CLASSE
# → aligné : les deux répondent OUTLIER pour les cas vagues
_SYSTEM_PROMPT_FS = (
    "Tu es un expert BCM (Banque Centrale de Mauritanie) spécialisé dans la classification "
    "des flux de transactions bancaires internationales selon la balance des paiements.\n"
    "RÈGLES STRICTES :\n"
    "- Tu choisis TOUJOURS un label parmi la liste fournie, sans exception.\n"
    "- Même si le libellé est abrégé, tronqué, en anglais ou mal orthographié, "
    "tu identifies le label sémantiquement le plus proche.\n"
    "- Si le libellé est ambigu ou trop vague pour appartenir clairement à un label "
    "économique précis (ex: 'PROJETS', 'VOYAGE D?AFFAIRES', 'ACTIVITES BANQUE CENTRALE', "
    "'PARTICULIERS ET PROFETIONNEL'), tu réponds OUTLIER.\n"
    "- Tu réponds UNIQUEMENT avec des lignes au format exact : N. LABEL\n"
    "- Une ligne par item, dans le même ordre. Zéro explication, zéro ligne vide."
)

_SYSTEM_PROMPT_FE = (
    "Tu es un expert BCM (Banque Centrale de Mauritanie) spécialisé dans la classification "
    "des flux de transactions bancaires internationales selon la balance des paiements.\n"
    "RÈGLES STRICTES :\n"
    "- Tu choisis TOUJOURS un label parmi la liste fournie, sans exception.\n"
    "- Même si le libellé est abrégé, tronqué, en anglais ou mal orthographié, "
    "tu identifies le label sémantiquement le plus proche.\n"
    "- Si le libellé est ambigu ou trop vague pour appartenir clairement à un label "
    "économique précis (ex: 'PROJETS', 'AFFAIRES', 'ACTIVITES BANQUE CENTRALE', "
    "'PARTICULIERS ET PROFETIONNEL', 'VIREMENT PERMANENT'), tu réponds OUTLIER.\n"
    "- Tu réponds UNIQUEMENT avec des lignes au format exact : N. LABEL\n"
    "- Une ligne par item, dans le même ordre. Zéro explication, zéro ligne vide."
)


# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════════════════════

class NatEcoReferentiel:
    """Contient toutes les données métier pour un flux donné (FS ou FE)."""

    def __init__(self, flux: str):
        self.flux = flux.upper()
        base      = Path(__file__).parent / "referentiel"

        # Référentiel depuis JSON
        ref_data         = json.load(open(base / f"referentiel_{self.flux}.json", encoding="utf-8"))
        self.referentiel = ref_data["referentiel"]

        # Few-shot depuis JSON
        self.few_shot = json.load(open(base / f"few_shot_{self.flux}.json", encoding="utf-8"))

        # Données métier depuis les constantes du code
        self.mapping_direct    = _MAPPING_DIRECT_FS    if self.flux == "FS" else _MAPPING_DIRECT_FE
        self.non_classe_direct = _NON_CLASSE_DIRECT_FS if self.flux == "FS" else _NON_CLASSE_DIRECT_FE
        self.system_prompt     = _SYSTEM_PROMPT_FS     if self.flux == "FS" else _SYSTEM_PROMPT_FE

        # Index inverse label → catégorie
        self.label_vers_categorie: dict[str, str] = {
            label: cat
            for cat, labels in self.referentiel.items()
            for label in labels
        }
        self.all_labels          = list(self.label_vers_categorie.keys())
        self.all_labels_set      = set(self.all_labels)
        self.liste_labels_prompt = "\n".join(f"{i+1}. {l}" for i, l in enumerate(self.all_labels))


# ══════════════════════════════════════════════════════════════════════════════
# WARM-START — cache de classification validée
# ══════════════════════════════════════════════════════════════════════════════

def load_warm_start(flux: str) -> dict[str, str]:
    """
    Charge validated_classif_{flux}.json.
    Retourne {modalite_clean: label_valide} ou {} si fichier vide/inexistant.
    """
    # Cherche d'abord validated_classif_NatEco_{flux}.json puis validated_classif_{flux}.json
    base = Path(__file__).parent / "referentiel"
    path = base / f"validated_classif_NatEco_{flux.upper()}.json"
    if not path.exists():
        path = base / f"validated_classif_{flux.upper()}.json"
    if not path.exists():
        return {}
    data   = json.load(open(path, encoding="utf-8"))
    classif = data.get("classif", {})
    # Re-indexer avec clé brut normalisée — plus robuste que nettoyer()
    # pour éviter les écarts d'encodage entre l'Excel v1 et les valeurs DB.
    return {k.strip().upper(): v for k, v in classif.items()}



# ══════════════════════════════════════════════════════════════════════════════
# NETTOYAGE
# ══════════════════════════════════════════════════════════════════════════════

def _normaliser(texte: str) -> str:
    nfkd     = unicodedata.normalize("NFKD", str(texte))
    sans_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^A-Z0-9\s]", " ", sans_acc.upper()).strip()


def nettoyer(texte: str) -> str:
    """
    Normalise + supprime préfixes numériques BCM + supprime tokens techniques.
    Logique identique au notebook.
    """
    t    = _normaliser(texte)
    t    = re.sub(r"^\d{1,5}[-\s]+", "", t.strip())
    mots = [m for m in t.split() if len(m) >= 2
            and not re.fullmatch(r"[\dA-Z]{1,3}\d+[\dA-Z]*", m)]
    return " ".join(mots)


# ══════════════════════════════════════════════════════════════════════════════
# DÉTECTION OUTLIER
# ══════════════════════════════════════════════════════════════════════════════

_MOTS_ADRESSE = {
    "ROAD","STREET","AVENUE","BOULEVARD","RUE","FLOOR","BUILDING",
    "BRANCH","DISTRICT","PROVINCE","CITY","CEDEX","BP","ROUTE",
    "NORTH","SOUTH","EAST","WEST","BOX","PLOT","UNIT","SUITE",
    "LEVEL","TOWER","INDUSTRIAL","COMPLEX","PLAZA","CENTER","CENTRE",
    "MUMBAI","DUBAI","LONDON","BEIJING","SHANGHAI","CAIRO","JEDDAH",
    "ISTANBUL","GUANGZHOU","HANGZHOU","YIWU","ZHEJIANG","DAKAR",
    "CASABLANCA","AGADIR","ALGER",
}

_PRENOMS_MAURITANIENS = {
    "AHMAD","AHMED","MOHAMMAD","MUHAMMAD","MOHAMED","MOUHAMED",
    "ABDALLAH","ABDALLAHI","ABDELLAHI","ABD","ABDERRAHMANE","ABDERAHMANE",
    "SIDI","BRAHIM","IBRAHIM","CHEIKH","SHEIKH",
    "OMAR","OUMAR","ALI","HASSAN","HUSSEIN","YAHYA","YAHIA",
    "MAMADOU","MOUSSA","ISSA","YOUSSEF","OUSMANE","YOUSSOUF","IDRISS",
    "SIDINA","TALEB","LELLAH","MALAININE","SALIHY","LEHBIB","HORMA",
    "MAHAND","IDOUMOU","BEYADE","KHALIFA","ELEMINE","YESAA","KHATRY",
    "HACHEM","MOCTAR","GHASSEM","YEHDHIH","EBDEMEL","BENNANE",
    "BOUCHRAYA","SALECK","THIAM","MOULAY","MAOULOUM","MAKE","MODY",
    "BAKARY","VELAH","DADDAH","EWFA","ASKER","GHOUEIZI","MELAININE",
    "HAMOUD","ZEINE","LEMINE","TOLBA","RYAN","BOUH","SOUMARE","CAMARA",
    "ZEINI","IMAM","MED","WELY","AMAL","EBNOU",
    "MARIEM","FATIMA","FATMA","LALLA","AMINATA","AISHA","KERTOUMA",
    "BABIYE","VATMA","LEJHOURY","MAADH","CHACH","SAVIYE","KHADIJETOU",
    "OUKADDOUR","NOUHAYLA",
    "EL","AL","OULD","MINT","BINT","BEN","BOU","SID","MME","MR",
}

_HEADERS_NATECO = {
    "NATUREECONOMIQUE","NATURE_ECONOMIQUE","NATURE ECONOMIQUE",
    "NON SPECIFIE","NON PRECISE","INCONNU","NS","ND","SNN",
    "STRING","NEANT","SANS OBJET","NULL","NONE","N/A","NAN","N A",
    "NON RENSEIGNE",
}


def _est_nom_propre(clean: str) -> bool:
    mots   = clean.split()
    if len(mots) < 2:
        return False
    connus = sum(1 for m in mots if m in _PRENOMS_MAURITANIENS)
    return connus >= max(1, len(mots) // 2)


def _est_date(clean: str) -> bool:
    return bool(re.search(
        r"\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}"
        r"|\d{4}[-/]\d{1,2}[-/]\d{1,2}"
        r"|[a-z]{3,4}[-/]\d{2,4}"
        r"|\d{1,2}[-/][a-z]{3,4})",
        clean, re.IGNORECASE,
    ))


def est_outlier(clean: str) -> bool:
    if not clean:
        return True
    if re.fullmatch(r"[\d\s]+", clean):
        return True
    if re.search(r"\d{4,}", clean):
        return True
    if set(clean.split()) & _MOTS_ADRESSE:
        return True
    if _est_date(clean):
        return True
    if _est_nom_propre(clean):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# RÈGLES SPÉCIALES
# ══════════════════════════════════════════════════════════════════════════════

def _est_vide(valeur) -> bool:
    if pd.isna(valeur):
        return True
    return str(valeur).strip() in ("", " ")


def appliquer_regle(
    row:   pd.Series,
    clean: str,
    ref:   NatEcoReferentiel,
) -> Optional[tuple[str, str]]:
    """
    Applique les règles spéciales. Retourne (categorie, label) ou None.

    Règles (même logique que le notebook) :
      1. ref == 'NA'  ET  nat == 'NA'   →  ('NA', 'NA')
      2. ref vide / nat vide/bruit      →  ('OUTLIER', 'OUTLIER')
      3. est_outlier(clean)             →  ('OUTLIER', 'OUTLIER')
      4. non_classe_direct              →  ('OUTLIER', 'OUTLIER')
    """
    ref_raw = str(row.get("ReferenceTransaction", "")).strip()
    nat_raw = str(row.get("NatureEconomique",     "")).strip()

    if ref_raw.upper() == "NA" and nat_raw.upper() == "NA":
        return ("NA", "NA")

    ref_vide = _est_vide(row.get("ReferenceTransaction")) or (
        ref_raw.upper() != "NA" and ref_raw.upper() in {"NULL", "NONE", "N/A", "NAN", ""}
    )
    nat_na = (not clean) or (clean in _HEADERS_NATECO)

    if ref_vide or nat_na:
        return ("OUTLIER", "OUTLIER")

    if est_outlier(clean):
        return ("OUTLIER", "OUTLIER")

    if clean in ref.non_classe_direct:
        return ("OUTLIER", "OUTLIER")

    return None


# ══════════════════════════════════════════════════════════════════════════════
# EMBEDDING
# ══════════════════════════════════════════════════════════════════════════════

def embed_labels_batch(
    cleans:         list[str],
    embed_model,
    embeddings_ref,
    all_labels:     list[str],
) -> list[tuple[str, float]]:
    """Similarité cosinus batch. Retourne [(label_best, score), ...]."""
    from sentence_transformers import util

    embeddings    = embed_model.encode(
        cleans,
        convert_to_tensor=True,
        show_progress_bar=False,
        normalize_embeddings=True,
        batch_size=256,
    )
    scores_matrix = util.dot_score(embeddings, embeddings_ref)

    return [
        (all_labels[scores.argmax().item()], scores.max().item())
        for scores in scores_matrix
    ]


# ══════════════════════════════════════════════════════════════════════════════
# POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def treating_nature_economique(
    df:          pd.DataFrame,
    nateco_col:  str              = "NatureEconomique",
    ref_col:     str              = "ReferenceTransaction",
    flux:        str              = "FS",
    ref:         NatEcoReferentiel = None,
    cfg:         dict             = None,
    warm_start:  bool             = False,
) -> pd.DataFrame:
    """
    Normalise le champ NatureEconomique.
    Traitement sur valeurs uniques — zéro redondance.

    Args:
        df         : DataFrame source
        nateco_col : colonne NatureEconomique brute
        ref_col    : colonne ReferenceTransaction
        flux       : "FS" ou "FE"
        ref        : NatEcoReferentiel — si None, chargé automatiquement selon flux
        cfg        : config LLM et embeddings (depuis le YAML via pipeline)

    Ajoute : NatEco_Clean, NatEco_Categorie, NatEco_Label, NatEco_Methode, NatEco_Check

    Args (supplémentaires) :
        warm_start : si True, charge validated_classif_{flux}.json et résout
                     directement les modalités déjà connues. Seules les nouvelles
                     modalités passent par la cascade (embed → LLM).
                     Activé via --warm-start dans le CLI.
    """
    from sentence_transformers import SentenceTransformer

    if ref is None:
        ref = NatEcoReferentiel(flux)

    if cfg is None:
        cfg = {}

    model_path        = cfg.get("model_embed",       r"C:\models\mpnet-base-v2")
    seuil_embed       = cfg.get("seuil_embed",       0.84)
    seuil_embed_court = cfg.get("seuil_embed_court", 0.95)
    batch_size_llm    = cfg.get("llm", {}).get("batch_size", 15)

    # Chargement modèle embedding
    print(f"  Chargement modèle : {model_path} ...")
    embed_model    = SentenceTransformer(model_path)
    embeddings_ref = embed_model.encode(
        ref.all_labels,
        convert_to_tensor=True,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    print(f"  Référentiel encodé : {len(ref.all_labels)} labels ({flux})")

    df = df.copy().reset_index(drop=True)
    n  = len(df)

    categories = [""] * n
    labels     = [""] * n
    methodes   = [""] * n
    checks     = ["OK"] * n
    cleans     = [""] * n

    # ── Warm-start : cache de classification validée ──────────────────────────
    # Si --warm-start actif, les modalités déjà connues sont résolues directement
    # depuis validated_classif_{flux}.json. Seules les nouvelles modalités
    # passent par la cascade complète (embed → LLM).
    ws_cache: dict[str, str] = {}
    ws_cache_upper: dict[str, str] = {}
    if warm_start:
        ws_cache = load_warm_start(flux)
        ws_cache_upper = {k.strip().upper(): v for k, v in ws_cache.items()}
        print(f"  Warm-start : {len(ws_cache)} modalités connues chargées ({flux})")

    # ── Étape 0 : nettoyage + règles spéciales ────────────────────────────────
    a_traiter = []
    for i, row in df.iterrows():
        nat       = row.get(nateco_col, "")
        clean     = nettoyer(str(nat)) if not pd.isna(nat) else ""
        cleans[i] = clean

        # ── Règle NA en PREMIER (avant tout, peu importe warm-start) ──────────
        # appliquer_regle() gère le cas NA/NA et tous les outliers bruts.
        # Cette étape ne peut pas être court-circuitée par le cache.
        regle = appliquer_regle(row, clean, ref)
        if regle:
            categories[i] = regle[0]
            labels[i]     = regle[1]
            methodes[i]   = "REGLE"
            continue

        # ── Warm-start : lookup sur brut normalisé (plus robuste que clean) ──
        # strip().upper() évite les écarts d'encodage/espaces entre Excel v1 et DB.
        # Warm-start lookup : 4 essais (exact / strip / rstrip / insensible casse)
        if warm_start:
            s      = str(row.get(nateco_col, ""))
            lbl_ws = None
            if s in ws_cache:                lbl_ws = ws_cache[s]
            elif s.strip() in ws_cache:      lbl_ws = ws_cache[s.strip()]
            elif s.rstrip() in ws_cache:     lbl_ws = ws_cache[s.rstrip()]
            else:
                key_up = s.strip().upper()
                if key_up in ws_cache_upper: lbl_ws = ws_cache_upper[key_up]
            if lbl_ws is not None:
                cat_ws        = ref.label_vers_categorie.get(lbl_ws, lbl_ws)
                categories[i] = cat_ws
                labels[i]     = lbl_ws
                methodes[i]   = "WARM"
                checks[i]     = "OK"
                cleans[i]     = nettoyer(s) if s else ""
                continue

        a_traiter.append(i)

    df["NatEco_Clean"] = cleans
    df["_ws_hit"] = [m == "WARM" for m in methodes]
    print(f"  Règles   : {n - len(a_traiter)} lignes")

    # ── Étape 0b : mapping direct ─────────────────────────────────────────────
    a_traiter_filtres = []
    for i in a_traiter:
        clean = cleans[i]
        if clean in ref.mapping_direct:
            lbl_direct = ref.mapping_direct[clean]
            if lbl_direct is None:
                categories[i] = "OUTLIER"
                labels[i]     = "OUTLIER"
                methodes[i]   = "REGLE"
            else:
                labels[i]     = lbl_direct
                categories[i] = ref.label_vers_categorie[lbl_direct]
                methodes[i]   = "EMBED"
        else:
            a_traiter_filtres.append(i)
    a_traiter = a_traiter_filtres

    # ── Étape 1 : dédoublonnage ───────────────────────────────────────────────
    modalites_index: dict[str, list[int]] = {}
    for i in a_traiter:
        modalites_index.setdefault(cleans[i], []).append(i)

    modalites_uniques = list(modalites_index.keys())
    print(f"  Lignes à classer : {len(a_traiter)} → {len(modalites_uniques)} modalités uniques")

    # ── Étape 2 : embedding ───────────────────────────────────────────────────
    resultats_embed = embed_labels_batch(modalites_uniques, embed_model, embeddings_ref, ref.all_labels)

    cache:         dict[str, tuple[str, str, str]] = {}
    a_envoyer_llm: list[str] = []

    for k, modalite in enumerate(modalites_uniques):
        lbl, score = resultats_embed[k]
        n_mots     = len(modalite.split())
        seuil      = seuil_embed if n_mots >= 3 else seuil_embed_court

        if score >= seuil:
            cache[modalite] = (ref.label_vers_categorie[lbl], lbl, "EMBED")
        else:
            a_envoyer_llm.append(modalite)

    # ── Étape 3 : LLM via shared/ollama_client ────────────────────────────────
    total_llm = len(a_envoyer_llm)
    for debut in range(0, total_llm, batch_size_llm):
        batch_val     = a_envoyer_llm[debut:debut + batch_size_llm]
        resultats_llm = call_llm_nateco_batch(
            batch            = batch_val,
            system_prompt    = ref.system_prompt,
            few_shot         = ref.few_shot,
            all_labels_set   = ref.all_labels_set,
            liste_labels_prompt = ref.liste_labels_prompt,
            cfg              = cfg,
        )
        for k, modalite in enumerate(batch_val):
            lbl = resultats_llm[k]
            cache[modalite] = (
                (ref.label_vers_categorie[lbl], lbl, "LLM") if lbl
                else ("AUTRES", "AUTRES", "LLM")
            )
        print(f"  [LLM] {min(debut + batch_size_llm, total_llm)}/{total_llm} modalités", end="\r")
    if total_llm:
        print()

    # ── Étape 4 : rediffusion cache ───────────────────────────────────────────
    for modalite, indices in modalites_index.items():
        cat, lbl, meth = cache[modalite]
        chk = "CHECK" if lbl == "AUTRES" else "OK"
        for i in indices:
            categories[i] = cat
            labels[i]     = lbl
            methodes[i]   = meth
            checks[i]     = chk

    df["NatEco_Categorie"] = categories
    df["NatEco_Label"]     = labels
    df["NatEco_Methode"]   = methodes
    df["NatEco_Check"]     = checks
    return df