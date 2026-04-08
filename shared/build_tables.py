"""
shared/build_tables.py
=======================
build_tables() — fonction commune à tous les pipelines.
Pattern identique pour Devise, Pays, TypeSwift, ModeReglement.
"""
from __future__ import annotations
import pandas as pd

def build_tables(df: pd.DataFrame, col_in: str, col_out: str,
                 ref_banque_col: str = "RefBanque",
                 outlier_tag: str = "OUTLIER") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retourne (df_clean, df_analysis).

    df_clean    : valeurs résolues (non OUTLIER), relation n→1, triées.
    df_analysis : statistiques OUTLIERS par banque (ou globales si RefBanque absente).
                  Colonnes : ref_banque_col, col_in, Nombre_OUTLIERS, Ratio_OUTLIERS,
                             Nombre_total_OUTLIERS, Nombre_total_CLEAN_VALUES, Nombre_total_LIGNES
    """
    df = df.copy()
    df_clean = (
        df[df[col_out] != outlier_tag][[col_in, col_out]]
        .drop_duplicates()
        .sort_values([col_out, col_in])
        .reset_index(drop=True)
    )
    df_out = df[df[col_out] == outlier_tag]
    has_rb = ref_banque_col in df.columns

    if has_rb:
        agg = (df_out.groupby([ref_banque_col, col_in], dropna=False)
               .size().reset_index(name="Nombre_OUTLIERS"))
        totals = df.groupby(ref_banque_col, dropna=False).agg(
            Nombre_total_OUTLIERS=(col_out, lambda x: (x == outlier_tag).sum()),
            Nombre_total_CLEAN_VALUES=(col_out, lambda x: (x != outlier_tag).sum()),
            Nombre_total_LIGNES=(col_out, "count"),
        ).reset_index()
        agg = agg.merge(totals, on=ref_banque_col, how="left")
        agg["Ratio_OUTLIERS"] = round(
            100 * agg["Nombre_OUTLIERS"] / agg["Nombre_total_LIGNES"].replace(0, 1), 2)
        df_analysis = agg[
            [ref_banque_col, col_in, "Nombre_OUTLIERS", "Ratio_OUTLIERS",
             "Nombre_total_OUTLIERS", "Nombre_total_CLEAN_VALUES", "Nombre_total_LIGNES"]
        ].sort_values([ref_banque_col, col_in]).reset_index(drop=True)
    else:
        agg = (df_out.groupby(col_in, dropna=False)
               .size().reset_index(name="Nombre_OUTLIERS"))
        n, no = len(df), len(df_out)
        agg["Ratio_OUTLIERS"]            = round(100 * agg["Nombre_OUTLIERS"] / max(n,1), 2)
        agg["Nombre_total_OUTLIERS"]     = no
        agg["Nombre_total_CLEAN_VALUES"] = n - no
        agg["Nombre_total_LIGNES"]       = n
        df_analysis = agg.sort_values(col_in).reset_index(drop=True)

    return df_clean, df_analysis
