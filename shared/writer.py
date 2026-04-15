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

def _parse_few_shot(few_shot: list[dict]) -> pd.DataFrame:
    """
    Transforme une liste few-shot [{"role":"user","content":...}, {"role":"assistant","content":...}]
    en DataFrame à deux colonnes : Input | Label_Attendu.
    Chaque ligne correspond à un exemple numéroté (N. valeur).
    """
    import re
    if not few_shot:
        return pd.DataFrame(columns=["Input", "Label_Attendu"])

    user_content  = next((m["content"] for m in few_shot if m.get("role") == "user"),  "")
    asst_content  = next((m["content"] for m in few_shot if m.get("role") == "assistant"), "")

    pattern = re.compile(r"^(\d+)\.\s*(.+)$", re.MULTILINE)
    inputs  = {int(m.group(1)): m.group(2).strip() for m in pattern.finditer(user_content)}
    labels  = {int(m.group(1)): m.group(2).strip() for m in pattern.finditer(asst_content)}

    rows = [{"Input": inputs[k], "Label_Attendu": labels.get(k, "")}
            for k in sorted(inputs)]
    return pd.DataFrame(rows)


def write_excel_sheets(
    frames:    dict,
    path:      Path,
    few_shot:  list[dict] | None = None,
) -> None:
    """
    Écrit plusieurs DataFrames dans un Excel multi-onglets (xlsxwriter).
    Division équitable si > EXCEL_MAX_ROWS lignes.
    En-têtes jaunes #FFD700, Arial 10, freeze A2.

    Onglets générés :
        1. Mapping_Clean
        2. Analyse_Outliers
        3. Instructions  — few-shot LLM si fourni, sinon vide
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

        # ── Onglet Instructions (few-shot si NatureEco, vide sinon) ──────────
        df_instr = _parse_few_shot(few_shot) if few_shot else pd.DataFrame(columns=["Input", "Label_Attendu"])
        df_instr.to_excel(writer, sheet_name="Instructions", index=False)
        ws_instr = writer.sheets["Instructions"]
        for ci, cn in enumerate(df_instr.columns):
            ws_instr.write(0, ci, cn, hfmt)
            ws_instr.set_column(ci, ci, min(len(str(cn)) + 4, 60))
        ws_instr.freeze_panes(1, 0)
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
# """
# shared/writer.py — Écriture CSV / Excel, commun à tous les pipelines.
# """
# from __future__ import annotations
# import math
# from pathlib import Path
# import pandas as pd

# EXCEL_MAX_ROWS = 1_048_576

# def write_csv(df: pd.DataFrame, path: Path) -> Path:
#     """Extraction → CSV (rapide, sans limite de lignes)."""
#     p = Path(path).with_suffix(".csv")
#     p.parent.mkdir(parents=True, exist_ok=True)
#     df.to_csv(p, index=False, encoding="utf-8-sig", sep=";")
#     return p

# def write_excel_sheets(frames: dict, path: Path, instructions=None) -> None:
#     """
#     Écrit plusieurs DataFrames dans un Excel multi-onglets (xlsxwriter).
#     Division équitable si > EXCEL_MAX_ROWS lignes.
#     En-têtes jaunes #FFD700, Arial 10, freeze A2.
#     """
#     path = Path(path)
#     path.parent.mkdir(parents=True, exist_ok=True)
#     with pd.ExcelWriter(str(path), engine="xlsxwriter") as writer:
#         hfmt = writer.book.add_format({
#             "bold": True, "font_name": "Arial", "font_size": 10,
#             "bg_color": "#FFD700", "align": "center", "valign": "vcenter", "border": 0,
#         })
#         for tab_base, df in frames.items():
#             n   = len(df)
#             ns  = max(1, math.ceil(n / (EXCEL_MAX_ROWS - 1)))
#             rps = math.ceil(n / ns)
#             for i in range(ns):
#                 chunk = df.iloc[i * rps: min((i + 1) * rps, n)]
#                 tab   = f"{tab_base}_Part_{i+1}" if ns > 1 else tab_base
#                 chunk.to_excel(writer, sheet_name=tab, index=False)
#                 ws = writer.sheets[tab]
#                 for ci, cn in enumerate(df.columns):
#                     ws.write(0, ci, cn, hfmt)
#                     ws.set_column(ci, ci, min(len(str(cn)) + 4, 60))
#                 ws.freeze_panes(1, 0)
