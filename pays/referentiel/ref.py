"""
normalize_pays_final.py  —  v5
================================
Pipeline de normalisation du champ "Pays".

COLONNES AJOUTÉES :
  Pays_clean   — valeur nettoyée
  Pays_iso2    — code ISO-2 / 'NoAs' / 'OUTLIER' / 'check' / None
  Pays_method  — 'MAP' / 'FUZZY' / 'ADDR' / 'LLM' / 'NoAs' / 'OUTLIER' / None / 'check'
  Pays_check   — True si revue manuelle nécessaire

RÈGLE NoAs / OUTLIER :
  NoAs    = Pays vide/null  ET  Ref vide/null  (les deux vides — bruit pur)
  OUTLIER = tout le reste qui n'est pas un pays valide :
              - Pays non vide mais non identifiable (adresse inconnue, mot-clé métier…)
              - Pays vide  ET  Ref non vide
              - Ref non vide ET Pays == "NA" → non, c'est la Namibie (iso NA)
              - Ref == "string" → OUTLIER direct

Dépendances :
    pip install pycountry babel geonamescache rapidfuzz pandas
"""

from __future__ import annotations

import re
import time
import unicodedata
from functools import lru_cache
from typing import Optional

import pandas as pd
import pycountry
from babel import Locale
import geonamescache
from rapidfuzz import process as rfuzz


# ══════════════════════════════════════════════════════════════════════════════
# 0.  REGEX PRÉ-COMPILÉES
# ══════════════════════════════════════════════════════════════════════════════

_RE_SYMBOLS     = re.compile(
    r"[.,/\\?;:!#@%&*()\[\]+=_~^`|<>'\"\u00b0\u2019\u2018\u2013\u2014\u00ab\u00bb]+"
)
_RE_SPACES      = re.compile(r"\s{2,}")
_RE_DIGITS_ONLY = re.compile(r"^\d+$")
_RE_POSTAL_LEAD = re.compile(r"^\d{3,}[\s\-]")
_RE_ADDR_START  = re.compile(
    r"^(\d{1,5}\s|NO\s|N\s|BP\s|PO BOX\s|ROUTE\s|RUE\s|AVENUE\s|"
    r"BOULEVARD\s|BD\s|STREET\s|ROAD\s|DRIVE\s|PLAZA\s|FLOOR\s|"
    r"UNIT\s|BUILDING\s|COMPLEX\s|INDUSTRIAL\s|SHOP\s|BLOCK\s)",
    re.IGNORECASE,
)
_RE_NON_PAYS    = re.compile(
    r"^(NONE|NULL|NA(?!MIBI)|N/A|STRING|IMMOBILIER|TOURISME|HOTELLIERE|"
    r"FAUX PARTICULIERS|INDUSTRIES TEXTILES|CONFECTION|AUTRES SERVICES|"
    r"MANUTENTION|COMMERCE DIVERS|COMMERCE ET L.INDUSTRIE|A NE PAS UTILISER|"
    r"INTERMEDIAIRES|COMCE GROS|FAUX|REG POLAIRES|ANTARCTIQUE|"
    r"REPUBLIC OF MORRIS|NOGAS)$",
    re.IGNORECASE,
)
_RE_DATE = re.compile(
    r"""\b(
        \d{4}[-/]\d{1,2}[-/]\d{1,2}
      | \d{1,2}[-/]\d{1,2}[-/]\d{2,4}
      | \d{1,2}[-/](jan|fev|mar|avr|mai|jun|jul|aou|sep|oct|nov|dec|
                    feb|apr|aug)[a-z]*[-/]?\d{0,4}
      | (jan|fev|mar|avr|mai|jun|jul|aou|sep|oct|nov|dec|
         feb|apr|aug)[a-z]*[-/\s]\d{2,4}
      | \d{1,2}-\d{2}
    )\b""",
    re.VERBOSE | re.IGNORECASE,
)

# Valeurs considérées comme "vides" dans ReferenceTransaction
_REF_EMPTY_VALUES = {"na", "nan", "none", "null", "", "string"}


# ══════════════════════════════════════════════════════════════════════════════
# 1.  UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════════

def _strip_acc(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

def _norm(s: str) -> str:
    return _strip_acc(s.strip().lower())

def _ref_is_empty(ref_raw: str) -> bool:
    """True si la référence transaction est considérée vide/invalide."""
    return ref_raw.strip().lower() in _REF_EMPTY_VALUES


# ══════════════════════════════════════════════════════════════════════════════
# 2.  NETTOYAGE  clean_pays()
# ══════════════════════════════════════════════════════════════════════════════

@lru_cache(maxsize=16_384)
def clean_pays(raw: str) -> str:
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = _RE_DATE.sub(" ", s)
    s = _RE_SYMBOLS.sub(" ", s)
    s = _RE_SPACES.sub(" ", s).strip()
    return _strip_acc(s.upper())


# ══════════════════════════════════════════════════════════════════════════════
# 3.  RÉFÉRENTIEL
# ══════════════════════════════════════════════════════════════════════════════

def _build_lookup() -> tuple[dict[str, str], set[str]]:
    raw: dict[str, str] = {}

    for c in pycountry.countries:
        a2 = c.alpha_2
        for attr in ("name", "alpha_2", "alpha_3", "official_name", "common_name"):
            v = getattr(c, attr, None)
            if v:
                raw[v] = a2

    fr_loc = Locale("fr")
    for c in pycountry.countries:
        fr_name = fr_loc.territories.get(c.alpha_2.upper())
        if fr_name:
            raw[fr_name] = c.alpha_2

    gc = geonamescache.GeonamesCache()
    for info in gc.get_countries().values():
        a2 = info.get("iso")
        for field in ("capital", "capital_fr"):
            cap = info.get(field)
            if cap and a2:
                raw[cap] = a2

    custom: dict[str, str] = {
        # ── États-Unis ──
        "etats-unis": "US", "etats-unis d'amerique": "US", "etats unis": "US",
        "etats unis d'amerique": "US", "usa": "US", "united states": "US",
        "ny": "US", "tx": "US", "fl": "US", "n y": "US",
        "new york": "US", "chicago": "US", "etats-unis amerique": "US",
        "420 montgomery street": "US",
        # ── Royaume-Uni ──
        "royaume-uni": "GB", "grande bretagne": "GB", "england": "GB", "uk": "GB",
        "united kingdom": "GB", "leicester le87 2bb united": "GB",
        # ── France ──
        "farance": "FR", "ffr": "FR", "bretagne": "FR",
        "5 rue scribe": "FR", "cedex france": "FR",
        # ── Allemagne ──
        "allemangne": "DE", "german": "DE", "deutschland": "DE",
        # ── Espagne ──
        "espana": "ES", "espange": "ES", "sp": "ES",
        "avenida diagonal 621 629": "ES", "las palmas": "ES",
        "las palmas espagne": "ES", "santa cruz de tenerife": "ES",
        # ── Italie ──
        "italia": "IT", "italya": "IT", "italla": "IT",
        "s.maria amonte (pi) branch": "IT", "via leonardo da vinci n8 5602": "IT",
        # ── Pays-Bas ──
        "pays-bas": "NL", "pays bas": "NL", "hollande": "NL", "holande": "NL",
        "holanda": "NL", "holland": "NL", "nederland": "NL",
        "the netherdands": "NL", "the netheriands": "NL", "the netherlnads": "NL",
        "netherland": "NL", "netherlande": "NL", "etherlands": "NL",
        # ── Suisse ──
        "swiss": "CH", "switezerland": "CH", "switzerlands": "CH",
        "pully": "CH", "chene bourg": "CH",
        # ── Belgique ──
        "belguim": "BE", "belgian": "BE", "andenne-belgique": "BE",
        # ── Norvège ──
        "norway": "NO",
        # ── Suède ──
        "sweden": "SE", "sw": "SE",
        # ── Danemark ──
        "denmark": "DK", "lyngby hovedgade 85 dk-2800": "DK",
        # ── Finlande ──
        "finland": "FI",
        # ── Irlande ──
        "ireland": "IE", "irland": "IE",
        # ── Portugal ──
        "portugual": "PT", "portugare": "PT",
        # ── Autriche ──
        "austria": "AT",
        # ── Luxembourg ──
        "luxerbourg": "LU",
        # ── Grèce ──
        "greece": "GR",
        # ── Pologne ──
        "poland": "PL", "poulanda": "PL", "polande": "PL",
        # ── Hongrie ──
        "hungary": "HU", "hangary": "HU",
        # ── Roumanie ──
        "romania": "RO",
        # ── Bulgarie ──
        "bulgaria": "BG",
        # ── Slovénie ──
        "slovenia": "SI", "slovenija": "SI",
        # ── Slovaquie ──
        "slovakia": "SK",
        # ── Rép. Tchèque ──
        "republique tcheque": "CZ", "tchequie": "CZ", "tcheque republique": "CZ",
        # ── Lituanie ──
        "lithuania": "LT",
        # ── Estonie ──
        "estonia": "EE",
        # ── Lettonie ──
        "latvia": "LV",
        # ── Chypre ──
        "cyprus": "CY",
        # ── Malte ──
        "malta": "MT",
        # ── Islande ──
        "iceland": "IS",
        # ── Ukraine ──
        "ukraine": "UA",
        # ── Russie ──
        "russie": "RU", "su": "RU",
        # ── Serbie ──
        "serbia": "RS", "serbie-et-montenegro": "RS",
        # ── Bosnie ──
        "bosnie herzegovine": "BA", "bosnie-herzegovine": "BA",
        "bosnia": "BA", "herzegovni": "BA",
        # ── Macédoine ──
        "macedoine": "MK", "macedoine ex-republique yougoslave": "MK",
        # ── Albanie ──
        "albania": "AL",
        # ── Moldavie ──
        "moldova": "MD",
        # ── Kosovo ──
        "kosovo": "XK",
        # ── Géorgie ──
        "georgia": "GE",
        # ── Arménie ──
        "armenia": "AM",
        # ── Azerbaïdjan ──
        "azerbaijan": "AZ",
        # ── Turquie ──
        "turkey": "TR", "turkiye": "TR", "turkya": "TR",
        "turkeye": "TR", "turkye": "TR",
        # ── Maroc ──
        "maroc": "MA", "marocco": "MA", "marroc": "MA",
        "casablanca": "MA", "mohammedia": "MA", "agadir": "MA",
        "laayoune": "MA", "maarif": "MA", "meknes": "MA", "bouskoura": "MA",
        "agence meknes ibn khaldoun": "MA",
        "aceur casablanca maroc": "MA", "nouaceur-casablanca": "MA",
        "20250 casablanca": "MA", "angle bdzerktouni rue franche": "MA",
        # ── Algérie ──
        "algerie": "DZ", "argerie": "DZ", "algeria": "DZ",
        "alger": "DZ", "algier": "DZ", "algerie": "DZ",
        "11 bd colonel amirouche alger": "DZ",
        # ── Tunisie ──
        "tunisia": "TN", "tunis": "TN", "sousse": "TN",
        "avenue habib bourguiba": "TN", "25 avenue habib bourguiba": "TN",
        "rue hedi nouira": "TN",
        # ── Égypte ──
        "egypt": "EG", "egybt": "EG", "eqypt": "EG", "caire": "EG",
        "86 cairo egypt": "EG", "86 cairo -alexandria egypt": "EG",
        "24 fawzy moaaz st semouha": "EG", "alexandria old port": "EG",
        # ── Libye ──
        "libya": "LY", "libyenne": "LY", "libyenne jamahiriya arabe": "LY",
        "libyan": "LY",
        # ── Mauritanie ──
        "mauritania": "MR", "mauritania,nouakchott": "MR",
        "nouakchott mauritania": "MR",
        "nktt": "MR", "teyarett amouratt lot 359": "MR",
        # ── Sahara Occidental ──
        "sahara occidental": "EH",
        # ── Sénégal ──
        "senegql": "SN", "senegale": "SN", "senegal residents": "SN",
        "dakar": "SN", "parcells assainies senegal": "SN",
        # ── Burkina Faso ──
        "burkina faso": "BF", "burkina": "BF",
        # ── Côte d'Ivoire ──
        "cote d'ivoire": "CI", "cote divoire": "CI", "cote d ivoire": "CI",
        "abidjan": "CI",
        # ── Cameroun ──
        "cameroun": "CM", "cameron": "CM",
        # ── RDC ──
        "rd congo": "CD", "republique democratique du congo": "CD",
        "rep democratique du congo": "CD", "congo rep democrat": "CD",
        # ── Maurice ──
        "mauritius": "MU", "iles maurices": "MU", "ile maurice": "MU",
        "republic of morris": "MU",
        # ── Afrique du Sud ──
        "south africa": "ZA", "rep of south africa": "ZA",
        "100 grayston drive sandton jhb": "ZA",
        # ── Kenya ──
        "kenya": "KE", "nairobi": "KE", "po box 30711 00100 nairobi": "KE",
        # ── Namibie ──
        "namibia": "NA", "namibie": "NA",
        # ── Bénin ──
        "benin": "BJ", "bénin": "BJ",
        # ── Botswana ──
        "bostwana": "BW",
        # ── Eswatini ──
        "swaziland": "SZ",
        # ── Arabie Saoudite ──
        "arabie saoudite": "SA", "arabie saudi": "SA", "ksa": "SA",
        "kingdom of saudi arabia": "SA", "arabe saoudi": "SA",
        "32040 jeddah 21428 saudi": "SA",
        # ── Émirats ──
        "emirats arabes unis": "AE", "uae": "AE", "uea": "AE",
        "dubai": "AE", "dubaie": "AE", "head office baniyas road": "AE",
        "jebel ali zone dubai": "AE", "v1a jumeriah lakes towers dubai": "AE",
        # ── Jordanie ──
        "jordan": "JO", "southern abdoun branch 17 maze": "JO",
        # ── Liban ──
        "lebanon": "LB", "lebenon": "LB", "leban": "LB",
        # ── Oman ──
        "oman": "OM", "sultanate of om": "OM",
        # ── Palestine ──
        "palestine": "PS",
        # ── Chine ──
        "chine populaire": "CN", "chinoi": "CN", "suzhou branch": "CN",
        "china heilongjiang branch": "CN",
        "43 renming road gongyi zhenhzh": "CN",
        "21 shishan road suzhou": "CN", "321 fengqi road hangzhou": "CN",
        "unit 5 duilding 9 jincun f": "CN", "n9 jinrong 2nd street wuxi": "CN",
        "line 307 mengcheng bozhou anhu": "CN",
        # ── Inde ──
        "indi": "IN", "ind": "IN",
        "wakadewai mumbai pune road": "IN",
        "taluk theni distirict india": "IN",
        "fort market branch mumbai indi": "IN",
        "complex morbi-36642 gujarat india": "IN",
        # ── Pakistan ──
        "pakistane": "PK",
        # ── Bangladesh ──
        "bangladech": "BD",
        # ── Thaïlande ──
        "thailand": "TH", "125 ekkachai rd bang bon": "TH",
        # ── Viêt Nam ──
        "vietnam": "VN",
        # ── Cambodge ──
        "cambodia": "KH",
        # ── Malaisie ──
        "malaysia": "MY", "malisya": "MY", "malasya": "MY",
        "jalan yap kwan 50450 kuala lumpur": "MY",
        "55 jalan raja chulan 50200": "MY",
        "50300 kuala lumpur malaysia": "MY",
        # ── Singapour ──
        "singapore": "SG", "singhaphore": "SG",
        "12 marina boulvard dbs asia": "SG",
        "exchange singapora 608526": "SG",
        "65chilia street ocbc centre": "SG",
        "1 wallich street 29 01 guoco": "SG",
        # ── Indonésie ──
        "indonesia": "ID", "indonossia": "ID",
        # ── Philippines ──
        "philipines": "PH",
        # ── Hong Kong ──
        "hong-kong": "HK", "hong kong": "HK", "hongkong": "HK", "honkong": "HK",
        "11th floor the center 99 queen": "HK",
        "3 garden road central hong kon": "HK",
        "tawer no 135hoi run rood hong kong": "HK",
        "hennessy road hong kong": "HK",
        "charter house 8 connaught road": "HK",
        # ── Taïwan ──
        "taiwan province de chine": "TW",
        # ── Corée ──
        "coree du sud": "KR", "coree republique de": "KR", 
        "south korea": "KR", 
        "coree du nord": "KP",
        "coree, rep. populaire democratique": "KP",
        # ── Japon ──
        "japon": "JP",
        # ── Ouzbékistan ──
        "uzbekistan": "UZ",
        # ── Canada ──
        "toronto": "CA",
        # ── Mexique ──
        "mexico": "MX",
        # ── Brésil ──
        "bresil": "BR", "brazil": "BR",
        # ── Argentine ──
        "argentina": "AR",
        # ── Chili ──
        "chile": "CL", "tchili": "CL",
        # ── Colombie ──
        "colombia": "CO",
        # ── Jamaïque ──
        "jamaica": "JM",
        # ── Australie ──
        "australia": "AU",
        # ── Nouvelle-Zélande ──
        "nouvelle zelande": "NZ", "new zealand": "NZ",
        # ── Territoires spéciaux ──
        "isle of man": "IM", "jersey": "JE", "gibraltar": "GI",
        "macao": "MO", "macau": "MO",
        "caïmans, ile": "KY",
        "an": "AN",
        "pb": "PG",
        # ── Antarctique ──
        "reg polaires, antarctique": "AQ", "antarctique": "AQ",

        "Maurice, Œle":"MU", "Maurice, Åle":"MI", "BÃ©nin":"BJ",


        "Afrique de l'ouset" : "OUTLIER", "KOREA" : "OUTLIER", "coree": "OUTLIER",
        "eu" : "OUTLIER", "korea" : "OUTLIER", "afrique" : "OUTLIER",
    }
    raw.update(custom)

    lookup: dict[str, str] = {}
    for k, v in raw.items():
        nk = _norm(str(k))
        if nk:
            lookup[nk] = v

    valid_iso2: set[str] = {c.alpha_2 for c in pycountry.countries} | {"XK", "AN"}
    return lookup, valid_iso2


_LOOKUP, _VALID_ISO2 = _build_lookup()
_REF_NAMES: list[str] = list(_LOOKUP.keys())


# ══════════════════════════════════════════════════════════════════════════════
# 4.  MOTS-CLÉS ADRESSE
# ══════════════════════════════════════════════════════════════════════════════

_ADDR_KEYWORDS: list[tuple[str, str]] = sorted(
    [
        # ── Noms de pays EN / FR ────────────────────────────────────────────
        ("saudi arabia", "SA"), ("united kingdom", "GB"), ("united states", "US"),
        ("south africa", "ZA"), ("new zealand", "NZ"), ("hong kong", "HK"),
        ("ivory coast", "CI"), ("cote d ivoire", "CI"),
        ("p r china", "CN"), ("p.r.china", "CN"),
        ("china", "CN"), ("france", "FR"), ("germany", "DE"),
        ("deutschland", "DE"), ("turkey", "TR"), ("turkiye", "TR"),
        ("india", "IN"), ("malaysia", "MY"), ("egypt", "EG"),
        ("morocco", "MA"), ("maroc", "MA"), ("saudi", "SA"),
        ("vietnam", "VN"), ("indonesia", "ID"), ("pakistan", "PK"),
        ("uae", "AE"), ("senegal", "SN"), ("mauritania", "MR"),
        ("mauritanie", "MR"), ("belgium", "BE"), ("netherlands", "NL"),
        ("singapore", "SG"), ("poland", "PL"), ("spain", "ES"),
        ("italy", "IT"), ("portugal", "PT"), ("switzerland", "CH"),
        ("austria", "AT"), ("ireland", "IE"), ("denmark", "DK"),
        ("norway", "NO"), ("sweden", "SE"), ("finland", "FI"),
        ("greece", "GR"), ("oman", "OM"), ("bahrain", "BH"),
        ("kuwait", "KW"), ("qatar", "QA"), ("jordan", "JO"),
        ("lebanon", "LB"), ("algerie", "DZ"), ("tunisie", "TN"),
        ("algeria", "DZ"), ("tunisia", "TN"), ("libya", "LY"),
        ("nigeria", "NG"), ("ghana", "GH"), ("kenya", "KE"),
        ("ethiopia", "ET"), ("cameroon", "CM"), ("cameroun", "CM"),
        ("senegal", "SN"), ("burkina", "BF"), ("mali", "ML"),
        ("niger", "NE"), ("tchad", "TD"), ("chad", "TD"),
        ("benin", "BJ"), ("togo", "TG"), ("guinea", "GN"),
        ("guinee", "GN"), ("angola", "AO"), ("mozambique", "MZ"),
        ("zambia", "ZM"), ("zimbabwe", "ZW"), ("rwanda", "RW"),
        ("uganda", "UG"), ("tanzania", "TZ"), ("congo", "CG"),
        ("gabon", "GA"), ("madagascar", "MG"), ("mauritius", "MU"),
        ("japan", "JP"), ("japon", "JP"), 
        ("thailand", "TH"), ("cambodia", "KH"), ("myanmar", "MM"),
        ("sri lanka", "LK"), ("bangladesh", "BD"), ("nepal", "NP"),
        ("afghanistan", "AF"), ("uzbekistan", "UZ"),
        ("kazakhstan", "KZ"), ("russia", "RU"), ("russie", "RU"),
        ("ukraine", "UA"), ("romania", "RO"), ("hungary", "HU"),
        ("czech", "CZ"), ("slovakia", "SK"), ("croatia", "HR"),
        ("serbia", "RS"), ("bulgaria", "BG"), ("albania", "AL"),
        ("estonia", "EE"), ("latvia", "LV"), ("lithuania", "LT"),
        ("luxembourg", "LU"), ("malta", "MT"), ("cyprus", "CY"),
        ("iceland", "IS"), ("canada", "CA"), ("mexico", "MX"),
        ("brazil", "BR"), ("bresil", "BR"), ("argentina", "AR"),
        ("chile", "CL"), ("colombia", "CO"), ("peru", "PE"),
        ("australia", "AU"), ("philippines", "PH"),
        # ── Villes — Afrique du Nord / Moyen-Orient ─────────────────────────
        ("casablanca", "MA"), ("rabat", "MA"), ("agadir", "MA"),
        ("fes", "MA"), ("marrakech", "MA"), ("tanger", "MA"),
        ("meknes", "MA"), ("oujda", "MA"), ("bouskoura", "MA"),
        ("mohammedia", "MA"), ("laayoune", "MA"),
        ("alger", "DZ"), ("oran", "DZ"), ("constantine", "DZ"),
        ("annaba", "DZ"), ("setif", "DZ"), ("ain el bey", "DZ"),
        ("tunis", "TN"), ("sfax", "TN"), ("sousse", "TN"),
        ("bizerte", "TN"), ("monastir", "TN"), ("kairouan", "TN"),
        ("tripoli", "LY"), ("benghazi", "LY"),
        ("cairo", "EG"), ("caire", "EG"), ("alexandria", "EG"),
        ("giza", "EG"), ("luxor", "EG"), ("aswan", "EG"),
        ("jeddah", "SA"), ("riyadh", "SA"), ("mecca", "SA"),
        ("makkah", "SA"), ("medina", "SA"), ("dammam", "SA"),
        ("khobar", "SA"), ("jubail", "SA"),
        ("dubai", "AE"), ("abu dhabi", "AE"), ("sharjah", "AE"),
        ("ajman", "AE"), ("fujairah", "AE"), ("ras al khaimah", "AE"),
        ("jumeriah", "AE"), ("jebel ali", "AE"),
        ("doha", "QA"), ("kuwait city", "KW"), ("manama", "BH"),
        ("muscat", "OM"), ("salalah", "OM"),
        ("amman", "JO"), ("beirut", "LB"), ("beyrouth", "LB"),
        ("damascus", "SY"), ("baghdad", "IQ"), ("basra", "IQ"),
        ("tehran", "IR"), ("istanbul", "TR"), ("ankara", "TR"),
        ("izmir", "TR"), ("gaziantep", "TR"), ("bursa", "TR"),
        ("adana", "TR"), ("konya", "TR"), ("antalya", "TR"),
        ("cekmekoy", "TR"), ("esenyurt", "TR"), ("balikesir", "TR"),
        # ── Villes — Afrique subsaharienne ──────────────────────────────────
        ("dakar", "SN"), ("abidjan", "CI"), ("lagos", "NG"),
        ("accra", "GH"), ("nairobi", "KE"), ("addis ababa", "ET"),
        ("yaounde", "CM"), ("douala", "CM"), ("ouagadougou", "BF"),
        ("bamako", "ML"), ("niamey", "NE"), ("ndjamena", "TD"),
        ("cotonou", "BJ"), ("lome", "TG"), ("conakry", "GN"),
        ("luanda", "AO"), ("maputo", "MZ"), ("lusaka", "ZM"),
        ("harare", "ZW"), ("kigali", "RW"), ("kampala", "UG"),
        ("dar es salaam", "TZ"), ("brazzaville", "CG"),
        ("kinshasa", "CD"), ("libreville", "GA"),
        ("antananarivo", "MG"), ("port louis", "MU"),
        ("johannesburg", "ZA"), ("cape town", "ZA"),
        ("durban", "ZA"), ("pretoria", "ZA"), ("sandton", "ZA"),
        ("nouakchott", "MR"),
        # ── Villes — Europe ─────────────────────────────────────────────────
        ("paris", "FR"), ("lyon", "FR"), ("marseille", "FR"),
        ("toulouse", "FR"), ("bordeaux", "FR"), ("lille", "FR"),
        ("nantes", "FR"), ("strasbourg", "FR"), ("grenoble", "FR"),
        ("montrouge", "FR"), ("nanterre", "FR"), ("antony", "FR"),
        ("villeurbanne", "FR"), ("palaiseau", "FR"), ("merignac", "FR"),
        ("london", "GB"), ("manchester", "GB"), ("birmingham", "GB"),
        ("edinburgh", "GB"), ("glasgow", "GB"), ("leeds", "GB"),
        ("bristol", "GB"), ("liverpool", "GB"), ("aberdeen", "GB"),
        ("leicester", "GB"),
        ("berlin", "DE"), ("munich", "DE"), ("hamburg", "DE"),
        ("frankfurt", "DE"), ("cologne", "DE"), ("dusseldorf", "DE"),
        ("stuttgart", "DE"), ("dortmund", "DE"), ("clausthal", "DE"),
        ("strabe", "DE"), ("wilhelm", "DE"),
        ("madrid", "ES"), ("barcelona", "ES"), ("seville", "ES"),
        ("valencia", "ES"), ("bilbao", "ES"), ("malaga", "ES"),
        ("zaragoza", "ES"), ("las palmas", "ES"),
        ("rome", "IT"), ("milan", "IT"), ("naples", "IT"),
        ("turin", "IT"), ("florence", "IT"), ("bologna", "IT"),
        ("amsterdam", "NL"), ("rotterdam", "NL"), ("hague", "NL"),
        ("brussels", "BE"), ("antwerp", "BE"), ("bruges", "BE"),
        ("zurich", "CH"), ("geneva", "CH"), ("bern", "CH"),
        ("lausanne", "CH"), ("pully", "CH"), ("chene bourg", "CH"),
        ("vienna", "AT"), ("graz", "AT"), ("salzburg", "AT"),
        ("lisbon", "PT"), ("porto", "PT"),
        ("stockholm", "SE"), ("gothenburg", "SE"), ("malmo", "SE"),
        ("oslo", "NO"), ("bergen", "NO"), ("trondheim", "NO"),
        ("copenhagen", "DK"), ("aarhus", "DK"),
        ("helsinki", "FI"), ("tampere", "FI"),
        ("dublin", "IE"), ("cork", "IE"),
        ("warsaw", "PL"), ("krakow", "PL"),
        ("prague", "CZ"), ("brno", "CZ"),
        ("budapest", "HU"), ("bucharest", "RO"),
        ("sofia", "BG"), ("athens", "GR"),
        ("moscow", "RU"), ("saint petersburg", "RU"),
        ("kyiv", "UA"), ("kiev", "UA"),
        ("luxembourg", "LU"), ("monaco", "MC"),
        ("belgrade", "RS"), ("zagreb", "HR"),
        # ── Villes — Asie ───────────────────────────────────────────────────
        ("beijing", "CN"), ("shanghai", "CN"), ("guangzhou", "CN"),
        ("shenzhen", "CN"), ("hangzhou", "CN"), ("suzhou", "CN"),
        ("yiwu", "CN"), ("wuhan", "CN"), ("chengdu", "CN"),
        ("nanjing", "CN"), ("jinan", "CN"), ("qingdao", "CN"),
        ("tianjin", "CN"), ("weihai", "CN"), ("xiamen", "CN"),
        ("dongguan", "CN"), ("foshan", "CN"), ("ningbo", "CN"),
        ("zhengzhou", "CN"), ("taizhou", "CN"), ("bozhou", "CN"),
        ("nanning", "CN"), ("gongyi", "CN"), ("jiaozuo", "CN"),
        ("zhejiang", "CN"), ("heilongjiang", "CN"), ("shandong", "CN"),
        ("jiangsu", "CN"), ("guangdong", "CN"), ("henan", "CN"),
        ("henaa", "CN"), ("hennan", "CN"),
        ("hong kong", "HK"), ("kowloon", "HK"),
        ("tokyo", "JP"), ("osaka", "JP"), ("yokohama", "JP"),
        ("nagoya", "JP"), ("kyoto", "JP"),
        ("seoul", "KR"), ("busan", "KR"), ("incheon", "KR"),
        ("taipei", "TW"), ("kaohsiung", "TW"),
        ("singapore", "SG"),
        ("kuala lumpur", "MY"), ("penang", "MY"), ("johor", "MY"),
        ("jakarta", "ID"), ("surabaya", "ID"), ("bandung", "ID"),
        ("bangkok", "TH"), ("pattaya", "TH"), ("chiang mai", "TH"),
        ("manila", "PH"), ("cebu", "PH"),
        ("hanoi", "VN"), ("ho chi minh", "VN"), ("saigon", "VN"),
        ("phnom penh", "KH"),
        ("mumbai", "IN"), ("delhi", "IN"), ("bangalore", "IN"),
        ("chennai", "IN"), ("hyderabad", "IN"), ("kolkata", "IN"),
        ("ahmedabad", "IN"), ("pune", "IN"), ("surat", "IN"),
        ("jaipur", "IN"), ("madurai", "IN"),
        ("karachi", "PK"), ("lahore", "PK"), ("islamabad", "PK"),
        ("dhaka", "BD"), ("chittagong", "BD"),
        ("colombo", "LK"), ("kathmandu", "NP"),
        ("tashkent", "UZ"), ("almaty", "KZ"),
        # ── Villes — Amériques ──────────────────────────────────────────────
        ("new york", "US"), ("chicago", "US"), ("los angeles", "US"),
        ("houston", "US"), ("miami", "US"), ("san francisco", "US"),
        ("washington", "US"), ("boston", "US"), ("dallas", "US"),
        ("toronto", "CA"), ("montreal", "CA"), ("vancouver", "CA"),
        ("mexico city", "MX"), ("guadalajara", "MX"),
        ("sao paulo", "BR"), ("rio de janeiro", "BR"),
        ("buenos aires", "AR"), ("santiago", "CL"),
        ("bogota", "CO"), ("lima", "PE"),
        # ── Villes — Océanie ────────────────────────────────────────────────
        ("sydney", "AU"), ("melbourne", "AU"), ("brisbane", "AU"),
        ("perth", "AU"), ("auckland", "NZ"),
        # ── Rues / avenues célèbres ─────────────────────────────────────────
        ("fenchurch street", "GB"), ("connaught road", "HK"),
        ("queen victoria", "HK"), ("robinson road", "SG"),
        ("brickell", "US"), ("montgomery street", "US"),
        ("rue scribe", "FR"), ("emile zola", "MA"),
        ("habib bourguiba", "TN"), ("hedi nouira", "TN"),
        ("colonel amirouche", "DZ"), ("ain el bey", "DZ"),
        ("rue de l arcade", "FR"), ("tourny", "FR"),
        ("al jezeera al hamra", "AE"), ("baniyas road", "AE"),
        ("jumeriah lakes", "AE"),
        ("shishan road", "CN"), ("fengqi road", "CN"),
        ("renming road", "CN"), ("yanjiang road", "CN"),
        ("qiutao road", "CN"), ("yincheng", "CN"),
        ("hualong", "CN"), ("jiangdong", "CN"), ("jinrong", "CN"),
        ("mengcheng", "CN"), ("ningnan", "CN"),
        ("ekkachai", "TH"), ("yap kwan", "MY"), ("raja chulan", "MY"),
        ("prinzregentenstr", "DE"), ("buyukdere", "TR"),
        ("grayston drive", "ZA"), ("haichang road", "CN"),
        ("heqing", "CN"), ("shifu dadao", "CN"),


        ("thunes","FR"), ("hay riad", "MA"), 
        ("tvz","MR"), ("dag hammarskjold plz","US"), ("souede","SE"), ("avenue de florissant","CH"), ("pierre burelle","FR"),
        ("av el brillante","ES"),
        ("cote diviore","CI"),
        ("port royal","FR"), ("fontaines","FR"),
        ("vaillant couturier","FR"), ("chemin de la matairie","FR"), ("lentisco","ES"),
        ("sevilla","ES"), ("voi cite de l observatoire","FR"), ("sunbury on thames","GB"), ("kultur mah","TR"),
        
        ("wisconsin ave ste","US"), ("ave joseph anoma","CI"), ("plantin en moretuslet","LU"), ("place gradens terrace","GB"),
        ("salahuddin al ar rawabi dis","SA"), ("rue des technologies 1 be 4432","BE"), ("50 rue carnot","FR"),
        ("co fitco ltd suite james","GB"),("albasalah st muslim word","SA"), ("12 rue des osiers","FR"),
        ("bldg no 84 west bay","AE"), ("6 karikal street","IN"), ("kiltur mah dr mediha eldem sk","TR"),
        ("qi maarouf 131 lot","MA"), ("s2 studio 155 loop street","ZA"),
        ("4 rue de la chine bat gali ecs 50046","FR"),
        ("haye riad 141 avenue des palmiers","MA"), ("beatriz de bobadilla","ES"), ("rue des technologies 1 be 4432","BE"), ("calle astros","ES"),
        ("boulevard pereire","FR"),("6 b rue de malleville","FR"),
        ("44 th street east between 1 and","US"), ("the quadrant","GB"), ("mers sultan","MA"),
        ("cornich boussoura matam","SN"),("platin en moretuslei","BE"), ("avenue du grey","FR"),
        ("rue de moliere","FR"), ("183 avenue georges almemenceau","FR"),
        ("avenue pierre m","FR"), ("olace felix baret","FR"),
        ("kuturmah","TR"), ("socogim de la plage nktt","MR"), ("bakanliklar","TR"), ("esoagne","ES"), 
        
        ("unit suite level bowman street","AU"), ("carel van bylandtlaan","NL"), ("unit suit lvl bowman st","AU"),
        ("church street samsung hub","SG"), ("appts z chat d'eau n ksar","MR"), ("dammstrasse zug zug","CH"),
        ("ave joseph anoma bp","CI"), ("alejandro hidalgo oficina","ES"), ("sharq area arabian gulf st","KW"),
        ("church street","SG"), ("avenue gabriel de ruminie","CH"), ("sharq area arabian gulf st","KW"),
        ("al morqab tower embassy street","QA"), ("kommekensstraat","BE"), ("sharq area arabian gulf","KW"),
        ("rue des filletes appartement b","FR"), ("al murqab tower","QA"), ("uadhibou pz provincia","MR"),
        ("rosehill pl dublin ca","US"), ("maliha road indl","AE"), ("dag hammarskjold plz el","US"),
        ("place victoria th floor","CA"), ("rebgasse","CH"), ("bldg road hoora","BH"),
        ("saint charles international","FR"), ("vila dr rihab ang","SA"), ("cl ignacio ellacuria beasco","ES"),
        ("boulvard yasser arafat bp","MR"), ("rue charles bonnet","CH"), ("office and charites committies sharq","KW"),
        ("lot numero cite mamelles aviation","SN"), ("mermoz","SN"), ("dist zipcode","SA"), ("sussex street","AU"), ("sharq area arabian","KW")
    ],
    key=lambda t: -len(t[0]),
)

def _extract_from_address(val_lower: str) -> Optional[str]:
    for kw, iso in _ADDR_KEYWORDS:
        if kw in val_lower:
            return iso
    return None


# ══════════════════════════════════════════════════════════════════════════════
# 5.  RÉSOLUTION  get_iso2_with_method()
# ══════════════════════════════════════════════════════════════════════════════

FUZZY_CUTOFF = 93   # bon compromis précision/rappel

@lru_cache(maxsize=16_384)
def get_iso2_with_method(raw_value: str) -> tuple[Optional[str], Optional[str]]:
    """
    Retourne (iso2, method).
    Cascade : non-pays → MAP exact → ISO-2 direct → FUZZY 93 → ADDR → check(LLM)
    """
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        return None, None

    cleaned = clean_pays(str(raw_value))
    if not cleaned:
        return None, None

    c_lower = _norm(cleaned)

    # 1. Valeur non-pays connue → OUTLIER
    if _RE_NON_PAYS.match(cleaned) or _RE_DIGITS_ONLY.match(cleaned):
        return "OUTLIER", "OUTLIER"

    # 2. Lookup exact → MAP
    if c_lower in _LOOKUP:
        return _LOOKUP[c_lower], "MAP"

    # 3. ISO-2 pur (2 lettres) → MAP
    if len(cleaned) == 2 and cleaned in _VALID_ISO2:
        return cleaned, "MAP"

    # 4. Fuzzy 93 — réduit les cas envoyés au LLM
    best = rfuzz.extractOne(c_lower, _REF_NAMES, score_cutoff=FUZZY_CUTOFF)
    if best:
        match_key, _, _ = best
        return _LOOKUP[match_key], "FUZZY"

    # 5. Extraction adresse (TOUJOURS tentée, pas seulement sur les longues valeurs)
    #    On cherche un mot-clé pays/ville dans n'importe quelle valeur non résolue
    iso = _extract_from_address(c_lower)
    if iso:
        return iso, "ADDR"

    # 6. Adresse longue non résolue par mots-clés → check (envoyé au LLM)
    return "check", "check"


# ══════════════════════════════════════════════════════════════════════════════
# 6.  RÈGLE NoAs / OUTLIER  (appliquée après résolution, avec ref_col)
#
#  Cas possibles :
#   Pays == "NoAs"  +  Ref vide/null/string  →  NoAs   (bruit confirmé)
#   Pays == "NoAs"  +  Ref non vide          →  OUTLIER
#   Pays vide/null  +  Ref vide/null/string  →  NoAs
#   Pays vide/null  +  Ref non vide          →  OUTLIER
#   Pays == "NA"    +  Ref non vide          →  "NA"   (Namibie)
#   Pays == "NA"    +  Ref vide              →  NoAs
#   Valeur non-pays (OUTLIER venant étape 1) →  OUTLIER (toujours, ref ou pas)
# ══════════════════════════════════════════════════════════════════════════════

def _apply_na_rule(
    row: pd.Series,
    pays_col: str,
    ref_col:  str,
) -> tuple[str, str]:
    """Retourne (iso2_final, method_final) après application de la règle NoAs/OUTLIER."""

    pays_raw    = str(row.get(pays_col, "")).strip()
    pays_lower  = pays_raw.lower()
    ref_raw     = str(row.get(ref_col,  "")).strip()
    ref_empty   = _ref_is_empty(ref_raw)

    current_iso = row["Pays_iso2"]
    current_mth = row["Pays_method"]

    # ── Valeur "NoAs" littérale dans Pays ──────────────────────────────────────
    # Si ref vide → NoAs confirmé (bruit pur)
    # Si ref présente → OUTLIER (pays invalide sur une vraie transaction)
    if pays_lower == "noas":
        if ref_empty:
            return "NoAs", "NoAs"
        return "OUTLIER", "OUTLIER"

    # ── Pays vide / null ────────────────────────────────────────────────────
    if pays_lower in ("", "na", "nan", "none", "null"):
        if pays_lower == "na" and not ref_empty:
            return "NA", "MAP"          # NA + ref valide = Namibie
        if ref_empty:
            return "NoAs", "NoAs"       # les deux vides → bruit pur
        return "OUTLIER", "OUTLIER"     # pays vide mais ref présente

    # ── Valeur non-pays détectée (OUTLIER depuis étape 1) ───────────────────
    if current_iso == "OUTLIER":
        return "OUTLIER", "OUTLIER"     # toujours OUTLIER, peu importe la ref

    # ── Ref vide mais pays présent → OUTLIER ────────────────────────────────
    # (transaction sans référence mais avec un pays — à investiguer)
    # Commenté par défaut : décommenter si on veut appliquer cette règle
    # if ref_empty:
    #     return "OUTLIER", "OUTLIER"

    return current_iso, current_mth


# ══════════════════════════════════════════════════════════════════════════════
# 7.  COUCHE QWEN via OLLAMA
# ══════════════════════════════════════════════════════════════════════════════

MODEL_LLM       = "qwen2.5:14b"
QWEN_MAX_RETRY  = 3
QWEN_RETRY_WAIT = 2

_SYSTEM_PAYS = """Tu es un assistant de normalisation de données bancaires.
On te donne une liste de valeurs extraites du champ "Pays" d'un fichier de transactions SWIFT.
Ces valeurs peuvent être : noms de pays (toutes langues), codes ISO, noms de villes,
adresses postales, noms de rues ou abréviations.

Pour chaque valeur :
- Si c'est un nom de pays ou un code ISO → retourne le code ISO-3166-1 alpha-2
- Si c'est un nom de ville, une adresse ou une rue → déduis le pays et retourne son ISO-2
- Si aucun indice géographique → retourne null

Réponds UNIQUEMENT par un objet JSON valide, sans texte ni balise markdown :
{"results": [{"input": "<valeur>", "iso2": "<CODE_ISO2_ou_null>"}]}

Règles :
- iso2 = code ISO-3166-1 alpha-2 en MAJUSCULES
- iso2 = null uniquement si vraiment impossible à déterminer
- Conserver strictement le même ordre et les mêmes valeurs "input" que l'entrée
"""

def _call_qwen_batch(values: list[str]) -> dict[str, Optional[str]]:
    import json
    import ollama

    user_content = json.dumps(
        {"values": [{"input": str(v)} for v in values]},
        ensure_ascii=False,
    )
    for attempt in range(1, QWEN_MAX_RETRY + 1):
        try:
            response = ollama.chat(
                model=MODEL_LLM,
                messages=[
                    {"role": "system", "content": _SYSTEM_PAYS},
                    {"role": "user",   "content": user_content},
                ],
                format="json",
                options={"temperature": 0},
            )
            raw_text = re.sub(r"```(?:json)?|```", "", response["message"]["content"]).strip()
            items    = json.loads(raw_text).get("results", [])
            mapping: dict[str, Optional[str]] = {}
            for item in items:
                inp = item.get("input", "")
                iso = item.get("iso2")
                mapping[inp] = str(iso).upper() if iso and str(iso).upper() in _VALID_ISO2 else None
            return mapping
        except Exception:
            if attempt < QWEN_MAX_RETRY:
                time.sleep(QWEN_RETRY_WAIT * attempt)
    return {v: None for v in values}


def enrich_with_llm(
    df: pd.DataFrame,
    iso_col:    str = "Pays_iso2",
    method_col: str = "Pays_method",
    pays_col:   str = "Pays",
    batch_size: int = 25,
) -> pd.DataFrame:
    check_mask   = (df[iso_col] == "check") & df[pays_col].notna() & (df[pays_col].astype(str).str.strip() != "")
    check_values = [str(v) for v in df.loc[check_mask, pays_col].unique().tolist()]

    if not check_values:
        df["Pays_check"] = df[iso_col] == "check"
        return df

    nb_batches = -(-len(check_values) // batch_size)
    full_map: dict[str, Optional[str]] = {}
    for i in range(0, len(check_values), batch_size):
        full_map.update(_call_qwen_batch(check_values[i: i + batch_size]))

    # Merge vectorisé sur valeurs uniques — zéro df.apply
    llm_df = pd.DataFrame([
        {pays_col: v,
         "_llm_iso": full_map.get(v),
         "_llm_mth": "LLM" if full_map.get(v) else "LLM_RATÉ"}
        for v in check_values
    ])

    df = df.merge(llm_df, on=pays_col, how="left")

    mask_check = df[iso_col] == "check"
    df.loc[mask_check, iso_col]    = df.loc[mask_check, "_llm_iso"].fillna("OUTLIER")
    df.loc[mask_check, method_col] = df.loc[mask_check, "_llm_mth"].fillna("LLM_RATÉ")

    df.drop(columns=["_llm_iso", "_llm_mth"], inplace=True)
    df["Pays_check"] = df[iso_col] == "OUTLIER"
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 8.  POINT D'ENTRÉE  treating_pays()
# ══════════════════════════════════════════════════════════════════════════════

def treating_pays(
    df: pd.DataFrame,
    pays_col:   str  = "Pays",
    ref_col:    str  = "ReferenceTransaction",
    use_llm:    bool = True,
    batch_size: int  = 25,
) -> pd.DataFrame:
    """
    Ajoute au DataFrame :
      Pays_clean   — valeur nettoyée
      Pays_iso2    — code ISO-2 / 'NoAs' / 'OUTLIER' / 'check' / None
      Pays_method  — méthode de résolution
      Pays_check   — True si revue manuelle nécessaire

    Optimisation : chaque étape travaille sur les VALEURS UNIQUES
    puis merge sur le DataFrame complet — zéro redondance.
    """
    df = df.copy()

    # ── ÉTAPE 1 : nettoyage sur valeurs uniques ───────────────────────────────
    unique_pays = df[pays_col].dropna().unique()
    clean_map   = {v: clean_pays(str(v)) for v in unique_pays}
    clean_map[None] = ""
    df["Pays_clean"] = df[pays_col].map(clean_map).fillna("")

    # ── ÉTAPE 2 : résolution ISO-2 sur valeurs uniques ────────────────────────
    # get_iso2_with_method est @lru_cache mais df.apply l'appelle N fois.
    # On l'appelle ici UNE SEULE FOIS par valeur unique puis on mappe.
    iso_map: dict = {}
    for v in unique_pays:
        iso_map[v] = get_iso2_with_method(str(v))

    df["Pays_iso2"]   = df[pays_col].map(lambda v: iso_map.get(v, (None, None))[0])
    df["Pays_method"] = df[pays_col].map(lambda v: iso_map.get(v, (None, None))[1])

    # ── ÉTAPE 3 : règle NoAs / OUTLIER sur paires uniques (pays, ref) ─────────
    # La règle dépend de DEUX colonnes → on déduplique sur les paires uniques.
    if ref_col in df.columns:
        pairs = df[[pays_col, ref_col]].drop_duplicates()

        pair_results: dict[tuple, tuple] = {}
        for _, row in pairs.iterrows():
            # On pré-remplit Pays_iso2 / Pays_method depuis iso_map
            fake_row = {
                pays_col:    row[pays_col],
                ref_col:     row[ref_col],
                "Pays_iso2": iso_map.get(row[pays_col], (None, None))[0],
                "Pays_method": iso_map.get(row[pays_col], (None, None))[1],
            }
            key = (row[pays_col], row[ref_col])
            pair_results[key] = _apply_na_rule(
                pd.Series(fake_row), pays_col, ref_col
            )

        # Merge vectorisé — on construit un df des paires résolues et on merge
        pair_df = pd.DataFrame([
            {pays_col: k[0], ref_col: k[1],
             "_pair_iso": v[0], "_pair_mth": v[1]}
            for k, v in pair_results.items()
        ])
        df = df.merge(pair_df, on=[pays_col, ref_col], how="left")
        df["Pays_iso2"]   = df["_pair_iso"].where(df["_pair_iso"].notna(), df["Pays_iso2"])
        df["Pays_method"] = df["_pair_mth"].where(df["_pair_mth"].notna(), df["Pays_method"])
        df.drop(columns=["_pair_iso", "_pair_mth"], inplace=True)

    # ── ÉTAPE 4 : LLM sur les "check" (déjà dédupliqué dans enrich_with_llm) ──
    if use_llm:
        df = enrich_with_llm(df, iso_col="Pays_iso2", method_col="Pays_method",
                             pays_col=pays_col, batch_size=batch_size)
    else:
        df["Pays_check"] = df["Pays_iso2"] == "check"

    return df



























{
  "_comment": "Mots-cles adresse pour la resolution du champ Pays. Modifiable sans toucher au code Python.",
  "version": "1.0.0",
  "addr_keywords": [
    ["habib bourguiba","TN"], ["rue de l arcade","FR"], ["av el brillante","ES"], ["avenue pierre m","FR"], ["kommekensstraat","BE"],
    ["al murqab tower","QA"], ["bldg road hoora","BH"], ["ras al khaimah","AE"], ["rio de janeiro","BR"], ["connaught road","HK"],
    ["queen victoria","HK"], ["jumeriah lakes","AE"], ["grayston drive","ZA"], ["pierre burelle","FR"], ["avenue du grey","FR"],
    ["rue de moliere","FR"], ["dar es salaam","TZ"], ["san francisco","US"], ["robinson road","SG"], ["yanjiang road","CN"],
    ["haichang road","CN"], ["50 rue carnot","FR"], ["church street","SG"], ["sussex street","AU"], ["saudi arabia","SA"],
    ["antananarivo","MG"], ["johannesburg","ZA"], ["villeurbanne","FR"], ["heilongjiang","CN"], ["kuala lumpur","MY"],
    ["buenos aires","AR"], ["baniyas road","AE"], ["shishan road","CN"], ["renming road","CN"], ["cote diviore","CI"],
    ["calle astros","ES"], ["the quadrant","GB"], ["dist zipcode","SA"], ["ivory coast","CI"], ["netherlands","NL"],
    ["switzerland","CH"], ["afghanistan","AF"], ["philippines","PH"], ["constantine","DZ"], ["kuwait city","KW"],
    ["addis ababa","ET"], ["ouagadougou","BF"], ["brazzaville","CG"], ["ho chi minh","VN"], ["los angeles","US"],
    ["mexico city","MX"], ["guadalajara","MX"], ["hedi nouira","TN"], ["fengqi road","CN"], ["qiutao road","CN"],
    ["raja chulan","MY"], ["shifu dadao","CN"], ["mers sultan","MA"], ["bakanliklar","TR"], ["mauritanie","MR"],
    ["mozambique","MZ"], ["madagascar","MG"], ["bangladesh","BD"], ["kazakhstan","KZ"], ["luxembourg","LU"],
    ["ain el bey","DZ"], ["alexandria","EG"], ["libreville","GA"], ["port louis","MU"], ["nouakchott","MR"],
    ["strasbourg","FR"], ["manchester","GB"], ["birmingham","GB"], ["dusseldorf","DE"], ["gothenburg","SE"],
    ["copenhagen","DK"], ["chiang mai","TH"], ["phnom penh","KH"], ["chittagong","BD"], ["washington","US"],
    ["rue scribe","FR"], ["emile zola","MA"], ["port royal","FR"], ["kultur mah","TR"], ["p r china","CN"],
    ["p.r.china","CN"], ["sri lanka","LK"], ["marrakech","MA"], ["abu dhabi","AE"], ["jebel ali","AE"],
    ["gaziantep","TR"], ["balikesir","TR"], ["cape town","ZA"], ["marseille","FR"], ["montrouge","FR"],
    ["palaiseau","FR"], ["edinburgh","GB"], ["liverpool","GB"], ["leicester","GB"], ["frankfurt","DE"],
    ["stuttgart","DE"], ["clausthal","DE"], ["barcelona","ES"], ["amsterdam","NL"], ["rotterdam","NL"],
    ["stockholm","SE"], ["trondheim","NO"], ["bucharest","RO"], ["guangzhou","CN"], ["zhengzhou","CN"],
    ["guangdong","CN"], ["kaohsiung","TW"], ["bangalore","IN"], ["hyderabad","IN"], ["ahmedabad","IN"],
    ["islamabad","PK"], ["kathmandu","NP"], ["vancouver","CA"], ["sao paulo","BR"], ["melbourne","AU"],
    ["jiangdong","CN"], ["mengcheng","CN"], ["buyukdere","TR"], ["fontaines","FR"], ["pakistan","PK"],
    ["portugal","PT"], ["ethiopia","ET"], ["cameroon","CM"], ["zimbabwe","ZW"], ["tanzania","TZ"],
    ["monastir","TN"], ["kairouan","TN"], ["benghazi","LY"], ["fujairah","AE"], ["jumeriah","AE"],
    ["beyrouth","LB"], ["damascus","SY"], ["istanbul","TR"], ["cekmekoy","TR"], ["esenyurt","TR"],
    ["ndjamena","TD"], ["kinshasa","CD"], ["pretoria","ZA"], ["toulouse","FR"], ["bordeaux","FR"],
    ["grenoble","FR"], ["nanterre","FR"], ["merignac","FR"], ["aberdeen","GB"], ["dortmund","DE"],
    ["valencia","ES"], ["zaragoza","ES"], ["florence","IT"], ["brussels","BE"], ["lausanne","CH"],
    ["salzburg","AT"], ["helsinki","FI"], ["budapest","HU"], ["belgrade","RS"], ["shanghai","CN"],
    ["shenzhen","CN"], ["hangzhou","CN"], ["dongguan","CN"], ["zhejiang","CN"], ["shandong","CN"],
    ["yokohama","JP"], ["surabaya","ID"], ["tashkent","UZ"], ["montreal","CA"], ["santiago","CL"],
    ["brisbane","AU"], ["auckland","NZ"], ["brickell","US"], ["yincheng","CN"], ["ekkachai","TH"],
    ["yap kwan","MY"], ["hay riad","MA"], ["lentisco","ES"], ["kuturmah","TR"], ["rebgasse","CH"],
    ["germany","DE"], ["morocco","MA"], ["senegal","SN"], ["belgium","BE"], ["bahrain","BH"],
    ["tunisie","TN"], ["nigeria","NG"], ["myanmar","MM"], ["croatia","HR"], ["bizerte","TN"],
    ["tripoli","LY"], ["sharjah","AE"], ["salalah","OM"], ["baghdad","IQ"], ["antalya","TR"],
    ["yaounde","CM"], ["cotonou","BJ"], ["conakry","GN"], ["kampala","UG"], ["sandton","ZA"],
    ["glasgow","GB"], ["bristol","GB"], ["hamburg","DE"], ["cologne","DE"], ["wilhelm","DE"],
    ["seville","ES"], ["bologna","IT"], ["antwerp","BE"], ["tampere","FI"], ["beijing","CN"],
    ["chengdu","CN"], ["nanjing","CN"], ["qingdao","CN"], ["tianjin","CN"], ["taizhou","CN"],
    ["nanning","CN"], ["jiaozuo","CN"], ["jiangsu","CN"], ["kowloon","HK"], ["incheon","KR"],
    ["jakarta","ID"], ["bandung","ID"], ["bangkok","TH"], ["pattaya","TH"], ["chennai","IN"],
    ["kolkata","IN"], ["madurai","IN"], ["karachi","PK"], ["colombo","LK"], ["houston","US"],
    ["hualong","CN"], ["jinrong","CN"], ["ningnan","CN"], ["sevilla","ES"], ["esoagne","ES"],
    ["france","FR"], ["kuwait","KW"], ["guinea","GN"], ["guinee","GN"], ["angola","AO"],
    ["zambia","ZM"], ["rwanda","RW"], ["uganda","UG"], ["russia","RU"], ["canada","CA"],
    ["tanger","MA"], ["annaba","DZ"], ["jeddah","SA"], ["riyadh","SA"], ["makkah","SA"],
    ["medina","SA"], ["dammam","SA"], ["khobar","SA"], ["jubail","SA"], ["manama","BH"],
    ["muscat","OM"], ["beirut","LB"], ["tehran","IR"], ["ankara","TR"], ["douala","CM"],
    ["bamako","ML"], ["niamey","NE"], ["luanda","AO"], ["maputo","MZ"], ["lusaka","ZM"],
    ["harare","ZW"], ["kigali","RW"], ["durban","ZA"], ["nantes","FR"], ["antony","FR"],
    ["london","GB"], ["berlin","DE"], ["munich","DE"], ["strabe","DE"], ["madrid","ES"],
    ["bilbao","ES"], ["malaga","ES"], ["naples","IT"], ["bruges","BE"], ["zurich","CH"],
    ["geneva","CH"], ["vienna","AT"], ["lisbon","PT"], ["bergen","NO"], ["aarhus","DK"],
    ["dublin","IE"], ["warsaw","PL"], ["krakow","PL"], ["prague","CZ"], ["athens","GR"],
    ["moscow","RU"], ["monaco","MC"], ["zagreb","HR"], ["suzhou","CN"], ["weihai","CN"],
    ["xiamen","CN"], ["foshan","CN"], ["ningbo","CN"], ["bozhou","CN"], ["gongyi","CN"],
    ["hennan","CN"], ["nagoya","JP"], ["taipei","TW"], ["penang","MY"], ["manila","PH"],
    ["saigon","VN"], ["mumbai","IN"], ["jaipur","IN"], ["lahore","PK"], ["almaty","KZ"],
    ["boston","US"], ["dallas","US"], ["bogota","CO"], ["sydney","AU"], ["tourny","FR"],
    ["heqing","CN"], ["thunes","FR"], ["souede","SE"], ["mermoz","SN"], ["china","CN"],
    ["india","IN"], ["saudi","SA"], ["spain","ES"], ["italy","IT"], ["qatar","QA"],
    ["ghana","GH"], ["niger","NE"], ["tchad","TD"], ["congo","CG"], ["gabon","GA"],
    ["japan","JP"], ["nepal","NP"], ["czech","CZ"], ["rabat","MA"], ["oujda","MA"],
    ["setif","DZ"], ["cairo","EG"], ["luxor","EG"], ["aswan","EG"], ["mecca","SA"],
    ["ajman","AE"], ["amman","JO"], ["basra","IQ"], ["izmir","TR"], ["bursa","TR"],
    ["adana","TR"], ["konya","TR"], ["lagos","NG"], ["accra","GH"], ["paris","FR"],
    ["lille","FR"], ["leeds","GB"], ["milan","IT"], ["turin","IT"], ["hague","NL"],
    ["porto","PT"], ["malmo","SE"], ["sofia","BG"], ["wuhan","CN"], ["jinan","CN"],
    ["henan","CN"], ["henaa","CN"], ["tokyo","JP"], ["osaka","JP"], ["kyoto","JP"],
    ["seoul","KR"], ["busan","KR"], ["johor","MY"], ["hanoi","VN"], ["delhi","IN"],
    ["surat","IN"], ["dhaka","BD"], ["miami","US"], ["perth","AU"], ["mali","ML"],
    ["chad","TD"], ["togo","TG"], ["peru","PE"], ["oran","DZ"], ["sfax","TN"],
    ["giza","EG"], ["doha","QA"], ["lome","TG"], ["lyon","FR"], ["rome","IT"],
    ["bern","CH"], ["graz","AT"], ["oslo","NO"], ["cork","IE"], ["brno","CZ"],
    ["kyiv","UA"], ["kiev","UA"], ["yiwu","CN"], ["cebu","PH"], ["pune","IN"],
    ["lima","PE"], ["fes","MA"], ["tvz","MR"],

    ["4 rue de la chine bat gali ecs 50046","FR"], ["office and charites committies sharq","KW"],
    ["haye riad 141 avenue des palmiers","MA"], ["lot numero cite mamelles aviation","SN"],
    ["44 th street east between 1 and","US"], ["rue des technologies 1 be 4432","BE"],
    ["183 avenue georges almemenceau","FR"], ["unit suite level bowman street","AU"],
    ["al morqab tower embassy street","QA"], ["rue des filletes appartement b","FR"],
    ["kiltur mah dr mediha eldem sk","TR"], ["salahuddin al ar rawabi dis","SA"],
    ["saint charles international","FR"], ["cl ignacio ellacuria beasco","ES"],
    ["voi cite de l observatoire","FR"], ["sharq area arabian gulf st","KW"],
    ["s2 studio 155 loop street","ZA"], ["church street samsung hub","SG"],
    ["appts z chat d'eau n ksar","MR"], ["alejandro hidalgo oficina","ES"],
    ["avenue gabriel de ruminie","CH"], ["boulvard yasser arafat bp","MR"],
    ["co fitco ltd suite james","GB"], ["albasalah st muslim word","SA"],
    ["socogim de la plage nktt","MR"], ["cornich boussoura matam","SN"],
    ["unit suit lvl bowman st","AU"], ["sharq area arabian gulf","KW"],
    ["dag hammarskjold plz el","US"], ["place victoria th floor","CA"],
    ["chemin de la matairie","FR"], ["plantin en moretuslet","LU"],
    ["place gradens terrace","GB"], ["6 b rue de malleville","FR"],
    ["carel van bylandtlaan","NL"], ["uadhibou pz provincia","MR"],
    ["rosehill pl dublin ca","US"], ["dag hammarskjold plz","US"],
    ["avenue de florissant","CH"], ["beatriz de bobadilla","ES"],
    ["platin en moretuslei","BE"], ["al jezeera al hamra","AE"],
    ["bldg no 84 west bay","AE"], ["dammstrasse zug zug","CH"],
    ["ave joseph anoma bp","CI"], ["vaillant couturier","FR"],
    ["qi maarouf 131 lot","MA"], ["rue charles bonnet","CH"],
    ["sharq area arabian","KW"], ["montgomery street","US"],
    ["colonel amirouche","DZ"], ["sunbury on thames","GB"],
    ["wisconsin ave ste","US"], ["12 rue des osiers","FR"],
    ["boulevard pereire","FR"], ["olace felix baret","FR"],
    ["vila dr rihab ang","SA"], ["saint petersburg","RU"],
    ["fenchurch street","GB"], ["prinzregentenstr","DE"],
    ["ave joseph anoma","CI"], ["6 karikal street","IN"],
    ["maliha road indl","AE"]
  ]
}












