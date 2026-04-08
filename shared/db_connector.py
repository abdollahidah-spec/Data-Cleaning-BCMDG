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


DB_USER     = "data_user"
DB_PASSWORD = "Bc@M!D32D"
DB_HOST     = "172.16.50.100"
DB_PORT     = "1433"
DB_NAME     = "DATAWAREHOUSE_SA_PROD"
DB_DRIVER   = "ODBC Driver 17 for SQL Server"


def _get_engine():
    """
    Crée et retourne un moteur SQLAlchemy vers la base SQL Server BCM.

    Returns:
        sqlalchemy.engine.Engine

    Raises:
        ImportError si sqlalchemy ou pyodbc ne sont pas installés.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    connection_url = URL.create(
        "mssql+pyodbc",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        query={"driver": DB_DRIVER},
    )
    return create_engine(connection_url)


def load_table(table_name: str) -> pd.DataFrame:
    """
    Charge toutes les colonnes d'une table SQL Server.

    Args:
        table_name : nom de la table (ex: 'E10FE')

    Returns:
        DataFrame complet de la table.
    """
    from sqlalchemy import text

    engine = _get_engine()
    query  = text(f"SELECT * FROM [DATAWAREHOUSE_SA_PROD].[dbo].[{table_name}]")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

'''
def get_engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    url = URL.create(
        "mssql+pyodbc",
        username=os.getenv("DB_USER"), 
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"), 
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"), 
        query={"driver": os.getenv("DB_DRIVER")})
    return create_engine(url)

def load_table(table_name: str) -> pd.DataFrame:
    """Charge toutes les colonnes d'une table SQL Server."""
    from sqlalchemy import text
    db = os.getenv("DB_NAME", "DATAWAREHOUSE_SA_PROD")
    with get_engine().connect() as conn:
        return pd.read_sql(text(f"SELECT * FROM [{db}].[dbo].[{table_name}]"), conn)
'''


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
