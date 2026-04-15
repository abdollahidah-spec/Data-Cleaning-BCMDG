"""
typeswift/pipeline_typeswift.py
================================
Pipeline TypeSwift — herite de BasePipeline.

Usage :
    python typeswift/pipeline_typeswift.py --config typeswift/config/E07_FS.yaml
    python typeswift/pipeline_typeswift.py --config typeswift/config/E10_FE.yaml
    python typeswift/pipeline_typeswift.py --all --config-dir typeswift/config/ --workers 2
    python typeswift/pipeline_typeswift.py --all --config-dir typeswift/config/ --warm-start
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
            swift_col  = cfg["columns"]["field"],
            ref_col    = cfg["columns"]["ref_transaction"],
            flux       = cfg["flux_type"],
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
        cfg["exclude_from_export"] = ["TypeSwift_clean", "TypeSwift_check", "_ws_hit"]
        return super().get_export_cols(df, cfg)


if __name__ == "__main__":
    import argparse, sys as _sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--config",      help="YAML d'une API")
    parser.add_argument("--config-dir",  default="typeswift/config/")
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--input",       default=None)
    parser.add_argument("--workers",     type=int, default=2)
    parser.add_argument("--schedule",    type=int, default=None)
    parser.add_argument("--warm-start",  action="store_true",
                        help="Resout directement depuis validated_classif avant la cascade.")
    args     = parser.parse_args()
    pipeline = TypeSwiftPipeline()

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