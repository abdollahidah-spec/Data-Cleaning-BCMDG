"""
devise/pipeline_devise.py
==========================
Pipeline Devise — hérite de BasePipeline.

Usage :
    python devise/pipeline_devise.py --config devise/config/E07_FS.yaml
    python devise/pipeline_devise.py --config devise/config/E09_PE.yaml --input data/ext.csv
    python devise/pipeline_devise.py --all --config-dir devise/config/ --workers 4
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.base_pipeline import BasePipeline
from shared.build_tables import build_tables
from devise.normalize_devise import treating_devise


class DevisePipeline(BasePipeline):

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        return treating_devise(
            df,
            devise_col = cfg["columns"]["field"],
            ref_col    = cfg["columns"]["ref_transaction"],
        )

    def build_output_tables(self, df: pd.DataFrame, cfg: dict):
        return build_tables(
            df,
            col_in         = cfg["columns"]["field"],
            col_out        = cfg["columns"]["field_out"],
            ref_banque_col = cfg["columns"].get("ref_banque", "RefBanque"),
            outlier_tag    = cfg.get("outlier_tag", "OUTLIER"),
        )

    def get_export_cols(self, df: pd.DataFrame, cfg: dict) -> list:
        # Extraction Devise : toutes colonnes + Devise_iso juste après Devise
        # On exclut Devise_clean, Devise_method, Devise_check de l'extraction
        cfg["exclude_from_export"] = ["Devise_clean", "Devise_method", "Devise_check"]
        return super().get_export_cols(df, cfg)


if __name__ == "__main__":
    DevisePipeline.cli(config_dir_default="devise/config/")
