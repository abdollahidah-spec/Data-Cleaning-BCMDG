"""
typeswift/pipeline_typeswift.py
================================
Pipeline TypeSwift — herite de BasePipeline.

Usage :
    python typeswift/pipeline_typeswift.py --config typeswift/config/E07_FS.yaml
    python typeswift/pipeline_typeswift.py --config typeswift/config/E10_FE.yaml
    python typeswift/pipeline_typeswift.py --all --config-dir typeswift/config/ --workers 2
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.base_pipeline import BasePipeline
from shared.build_tables import build_tables
from typeswift.normalize_typeswift import treating_typeswift


class TypeSwiftPipeline(BasePipeline):

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        return treating_typeswift(
            df,
            swift_col = cfg["columns"]["field"],
            ref_col   = cfg["columns"]["ref_transaction"],
            flux      = cfg["flux_type"],          
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
        cfg["exclude_from_export"] = ["TypeSwift_clean", "TypeSwift_check"]
        return super().get_export_cols(df, cfg)


if __name__ == "__main__":
    TypeSwiftPipeline.cli(config_dir_default="typeswift/config/")