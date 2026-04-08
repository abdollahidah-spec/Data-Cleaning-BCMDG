"""
mode_reglement/pipeline_mode_reglement.py
==========================================
Pipeline ModeReglement — herite de BasePipeline.

Usage :
    python mode_reglement/pipeline_mode_reglement.py --config mode_reglement/config/E07_FS.yaml
    python mode_reglement/pipeline_mode_reglement.py --all --config-dir mode_reglement/config/
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.base_pipeline import BasePipeline
from shared.build_tables import build_tables
from mode_reglement.normalize_mode_reglement import treating_mode_reglement


class ModeReglementPipeline(BasePipeline):

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        return treating_mode_reglement(
            df,
            mode_col = cfg["columns"]["field"],
            ref_col  = cfg["columns"]["ref_transaction"],
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
        cfg["exclude_from_export"] = ["ModeReglement_clean", "ModeReglement_check"]
        return super().get_export_cols(df, cfg)


if __name__ == "__main__":
    ModeReglementPipeline.cli(config_dir_default="mode_reglement/config/")
