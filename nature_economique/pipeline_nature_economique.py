"""
nature_economique/pipeline_nature_economique.py
=================================================
Pipeline NatureEconomique — hérite de BasePipeline.

Usage :
    python nature_economique/pipeline_nature_economique.py --config nature_economique/config/E07_FS.yaml
    python nature_economique/pipeline_nature_economique.py --config nature_economique/config/E10_FE.yaml
    python nature_economique/pipeline_nature_economique.py --all --config-dir nature_economique/config/ --workers 2
    python nature_economique/pipeline_nature_economique.py --all --config-dir nature_economique/config/ --warm-start
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.base_pipeline import BasePipeline
from shared.build_tables import build_tables
from nature_economique.normalize_nature_economique import (
    NatEcoReferentiel,
    treating_nature_economique,
)


class NatureEconomiquePipeline(BasePipeline):

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        # Chargement du référentiel selon le flux (FS ou FE)
        flux        = cfg["flux_type"]
        ref = NatEcoReferentiel(flux)

        # Config LLM et embedding depuis le YAML (surcharge config_base)
        embed_cfg = {
            "model_embed":       cfg.get("model_embed",      r"C:\models\mpnet-base-v2"),
            "seuil_embed":       cfg.get("seuil_embed",      0.84),
            "seuil_embed_court": cfg.get("seuil_embed_court",0.95),
            "model_llm":         cfg["llm"]["model"],
            "batch_size_llm":    cfg["llm"].get("batch_size", 15),
            "tokens_par_ligne":  cfg.get("tokens_par_ligne", 35),
        }

        result = treating_nature_economique(
            df,
            nateco_col  = cfg["columns"]["field"],
            ref_col     = cfg["columns"]["ref_transaction"],
            flux        = flux,
            ref         = ref,
            cfg         = embed_cfg,
            warm_start  = cfg.get("warm_start", False),
        )
        # Stocker le few-shot pour l'onglet Instructions du fichier Excel
        self._few_shot = ref.few_shot
        return result

    def build_output_tables(self, df: pd.DataFrame, cfg: dict):
        col_in      = cfg["columns"]["field"]
        col_out     = cfg["columns"]["field_out"]
        outlier_tag = cfg.get("outlier_tag", "OUTLIER")

        df_clean, df_analysis = build_tables(
            df,
            col_in         = col_in,
            col_out        = col_out,
            ref_banque_col = cfg["columns"].get("ref_banque", "RefBanque"),
            outlier_tag    = outlier_tag,
        )

        # Ajouter NatEco_Categorie dans le Mapping_Clean
        if "NatEco_Categorie" in df.columns:
            cat_map = (
                df[df[col_out] != outlier_tag][[col_in, "NatEco_Categorie"]]
                .drop_duplicates()
            )
            df_clean = df_clean.merge(cat_map, on=col_in, how="left")
            # Réordonner : NatureEconomique | NatEco_Categorie | NatEco_normalisee
            cols = [col_in, "NatEco_Categorie", col_out]
            df_clean = df_clean[cols]

        return df_clean, df_analysis

    def get_export_cols(self, df: pd.DataFrame, cfg: dict) -> list:
        col_in   = cfg["columns"]["field"]          # NatureEconomique (brut)
        col_cat  = "NatEco_Categorie"
        col_out  = cfg["columns"]["field_out"]      # NatEco_normalisee
        exclude  = {"NatEco_Clean", "NatEco_Check", "_ws_hit", col_cat, col_out}
        avail    = [c for c in df.columns if c not in exclude]
        idx      = avail.index(col_in) + 1 if col_in in avail else len(avail)
        # Ordre : ... NatureEconomique | NatEco_Categorie | NatEco_Label ...
        extra = [col_cat] if col_cat in df.columns else []
        return avail[:idx] + extra + [col_out] + avail[idx:]


if __name__ == "__main__":
    import argparse, sys
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",      help="YAML d'une API")
    parser.add_argument("--config-dir",  default="nature_economique/config/")
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--input",       default=None)
    parser.add_argument("--workers",     type=int, default=2)
    parser.add_argument("--warm-start",  action="store_true",
                        help="Utilise validated_classif_{flux}.json pour résoudre "
                             "directement les modalités déjà connues. "
                             "Seules les nouvelles modalités passent par embed → LLM.")
    args = parser.parse_args()

    pipeline = NatureEconomiquePipeline()

    # Injecter warm_start dans le contexte via monkeypatch de load_config
    if args.warm_start:
        from shared.base_pipeline import load_config as _orig_load_config
        def _patched_load_config(path):
            cfg = _orig_load_config(path)
            cfg["warm_start"] = True
            return cfg
        import nature_economique.pipeline_nature_economique as _self_mod
        import shared.base_pipeline as _bp
        _bp.load_config = _patched_load_config

    if args.all:
        pipeline.run_all(args.config_dir, max_workers=args.workers)
    elif args.config:
        pipeline.run(args.config, override_input=args.input)
    else:
        parser.print_help()
        sys.exit(1)