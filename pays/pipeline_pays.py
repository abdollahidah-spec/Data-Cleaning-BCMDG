"""
pays/pipeline_pays.py
======================
Pipeline Pays — herite de BasePipeline.
Utilise Ollama/Qwen pour les cas non resolus (use_llm: true dans le YAML).

Usage :
    python pays/pipeline_pays.py --config pays/config/E07_FS.yaml
    python pays/pipeline_pays.py --config pays/config/E10_FE.yaml --input data/ext.csv
    python pays/pipeline_pays.py --all --config-dir pays/config/ --workers 2
    python pays/pipeline_pays.py --all --config-dir pays/config/ --warm-start
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
            warm_start = cfg.get("warm_start", False),
            api_id     = cfg.get("api_id", "E07_FS"),
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
        exclude = {"Pays_clean", "Pays_check", "_ws_hit"}
        avail   = [c for c in df.columns if c not in exclude and c != col_out
                   and c != "Pays_method"]
        idx     = avail.index(col_in) + 1 if col_in in avail else len(avail)
        return avail[:idx] + [col_out, "Pays_method"] + avail[idx:]


if __name__ == "__main__":
    import argparse, sys as _sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--config",      help="YAML d'une API")
    parser.add_argument("--config-dir",  default="pays/config/")
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--input",       default=None)
    parser.add_argument("--workers",     type=int, default=2)
    parser.add_argument("--schedule",    type=int, default=None)
    parser.add_argument("--warm-start",  action="store_true",
                        help="Resout depuis validated_classif_Pays_{FS|OCD|FE}.json "
                             "avant la cascade normale.")
    args     = parser.parse_args()
    pipeline = PaysPipeline()

    def _run_once():
        if args.all:
            pipeline.run_all(args.config_dir, max_workers=args.workers, warm_start=args.warm_start)
        elif args.config:
            pipeline.run(args.config, override_input=args.input, warm_start=args.warm_start)
        else:
            parser.print_help(); _sys.exit(1)

    if args.schedule:
        import time
        print(f"Mode planifie : toutes les {args.schedule} jour(s). Ctrl+C pour arreter.")
        while True:
            _run_once()
            time.sleep(args.schedule * 86400)
    else:
        _run_once()