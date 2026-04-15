"""
devise/pipeline_devise.py
==========================
Pipeline Devise — hérite de BasePipeline.

Usage :
    python devise/pipeline_devise.py --config devise/config/E07_FS.yaml
    python devise/pipeline_devise.py --config devise/config/E09_PE.yaml --input data/ext.csv
    python devise/pipeline_devise.py --all --config-dir devise/config/ --workers 4
    python devise/pipeline_devise.py --all --config-dir devise/config/ --warm-start
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
            warm_start = cfg.get("warm_start", False),
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
        cfg["exclude_from_export"] = ["Devise_clean", "Devise_method", "Devise_check", "_ws_hit"]
        return super().get_export_cols(df, cfg)


if __name__ == "__main__":
    import argparse, sys as _sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--config",      help="YAML d'une API")
    parser.add_argument("--config-dir",  default="devise/config/")
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--input",       default=None)
    parser.add_argument("--workers",     type=int, default=4)
    parser.add_argument("--schedule",    type=int, default=None)
    parser.add_argument("--warm-start",  action="store_true",
                        help="Résout directement depuis validated_classif.json "
                             "avant la cascade normale.")
    args = parser.parse_args()

    pipeline = DevisePipeline()

    def _run_once():
        if args.all:
            pipeline.run_all(
                args.config_dir,
                max_workers=args.workers,
                warm_start=args.warm_start,
            )
        elif args.config:
            pipeline.run(
                args.config,
                override_input=args.input,
                warm_start=args.warm_start,
            )
        else:
            parser.print_help(); _sys.exit(1)

    if args.schedule:
        import time
        print(f"Mode planifié : toutes les {args.schedule} jour(s). Ctrl+C pour arrêter.")
        while True:
            _run_once()
            print(f"  Prochaine exécution dans {args.schedule} jour(s).")
            time.sleep(args.schedule * 86400)
    else:
        _run_once()