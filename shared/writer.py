"""
shared/writer.py — Écriture CSV / Excel, commun à tous les pipelines.
"""
from __future__ import annotations
import math
from pathlib import Path
import pandas as pd

EXCEL_MAX_ROWS = 1_048_576

def write_csv(df: pd.DataFrame, path: Path) -> Path:
    """Extraction → CSV (rapide, sans limite de lignes)."""
    p = Path(path).with_suffix(".csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, encoding="utf-8-sig", sep=";")
    return p

def write_excel_sheets(frames: dict, path: Path) -> None:
    """
    Écrit plusieurs DataFrames dans un Excel multi-onglets (xlsxwriter).
    Division équitable si > EXCEL_MAX_ROWS lignes.
    En-têtes jaunes #FFD700, Arial 10, freeze A2.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(str(path), engine="xlsxwriter") as writer:
        hfmt = writer.book.add_format({
            "bold": True, "font_name": "Arial", "font_size": 10,
            "bg_color": "#FFD700", "align": "center", "valign": "vcenter", "border": 0,
        })
        for tab_base, df in frames.items():
            n   = len(df)
            ns  = max(1, math.ceil(n / (EXCEL_MAX_ROWS - 1)))
            rps = math.ceil(n / ns)
            for i in range(ns):
                chunk = df.iloc[i * rps: min((i + 1) * rps, n)]
                tab   = f"{tab_base}_Part_{i+1}" if ns > 1 else tab_base
                chunk.to_excel(writer, sheet_name=tab, index=False)
                ws = writer.sheets[tab]
                for ci, cn in enumerate(df.columns):
                    ws.write(0, ci, cn, hfmt)
                    ws.set_column(ci, ci, min(len(str(cn)) + 4, 60))
                ws.freeze_panes(1, 0)
