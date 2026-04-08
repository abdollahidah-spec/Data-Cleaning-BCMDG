"""
pays/pipeline_pays.py
======================
Pipeline Pays — hérite de BasePipeline.
Utilise Ollama/Qwen pour les cas non résolus (use_llm: true dans le YAML).

Usage :
    python pays/pipeline_pays.py --config pays/config/E07_FS.yaml
    python pays/pipeline_pays.py --config pays/config/E10_FE.yaml --input data/ext.csv
    python pays/pipeline_pays.py --all --config-dir pays/config/ --workers 2
"""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.base_pipeline import BasePipeline
from shared.build_tables import build_tables
from pays.normalize_pays import treating_pays


class PaysPipeline(BasePipeline):

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        return treating_pays(
            df,
            pays_col   = cfg["columns"]["field"],
            ref_col    = cfg["columns"]["ref_transaction"],
            use_llm    = cfg.get("use_llm", False),
            batch_size = cfg.get("llm", {}).get("batch_size", 25),
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
        col_in  = cfg["columns"]["field"]
        col_out = cfg["columns"]["field_out"]
        # Pour Pays on garde aussi Pays_method dans l'extraction
        exclude = {"Pays_clean", "Pays_check"}
        avail   = [c for c in df.columns if c not in exclude and c != col_out
                   and c != "Pays_method"]
        idx     = avail.index(col_in) + 1 if col_in in avail else len(avail)
        return avail[:idx] + [col_out, "Pays_method"] + avail[idx:]


if __name__ == "__main__":
    PaysPipeline.cli(config_dir_default="pays/config/")
