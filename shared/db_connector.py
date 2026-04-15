"""
shared/db_connector.py — Connexion SQL Server et chargement de données.
Credentials chargés depuis .env.
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

def get_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    url = URL.create("mssql+pyodbc",
        username=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"), query={"driver": driver})
    return create_engine(url)

_DATE_FILTER_FIELDS = {"pays", "natureeconomique"}

def load_table(table_name: str, field: str = "") -> pd.DataFrame:
    """
    Charge toutes les colonnes d'une table SQL Server.

    Si le champ demandé est Pays ou NatureEconomique, applique automatiquement
    un filtre sur dtCr > '2024-01-01' pour limiter le volume de données.
    """
    from sqlalchemy import text
    db      = os.getenv("DB_NAME", "DATAWAREHOUSE_SA_PROD")
    where   = ""
    if field.lower() in _DATE_FILTER_FIELDS:
        where = " WHERE dtCr > '2024-01-01'"
    query = f"SELECT * FROM [{db}].[dbo].[{table_name}]{where}"
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn)

def load_file(path: str, cfg: dict) -> pd.DataFrame:
    """Charge un fichier CSV ou Excel."""
    p = Path(path)
    inp = cfg.get("input", {})
    if not p.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(p, sheet_name=inp.get("sheet", 0), dtype=str)
    if p.suffix.lower() in (".csv", ".tsv"):
        return pd.read_csv(p, sep=inp.get("sep", ";"),
                           encoding=inp.get("encoding", "utf-8-sig"), dtype=str)
    raise ValueError(f"Format non supporté : {p.suffix}")