"""
shared/base_pipeline.py
========================
Classe de base pour tous les pipelines BCM Data Governance.

Chaque pipeline_X.py hérite de BasePipeline et implémente :
    normalize(df, cfg)           → df enrichi des colonnes normalisées
    build_output_tables(df, cfg) → (df_clean, df_analysis)

CLI intégré — usage depuis le terminal :
    python devise/pipeline_devise.py --config devise/config/E07_FS.yaml
    python pays/pipeline_pays.py     --config pays/config/E10_FE.yaml --input data/ext.csv
    python typeswift/pipeline_typeswift.py --all --config-dir typeswift/config/ --workers 3
"""
from __future__ import annotations

import argparse, sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

# ══════════════════════════════════════════════════════════════════════════════
# RÈGLE NA — commune à tous les champs
# ══════════════════════════════════════════════════════════════════════════════

def apply_na_rule(
    row:       "pd.Series",
    field_col: str,
    ref_col:   str,
    iso_col:   str,
    mth_col:   str,
) -> tuple:
    """
    Règle NA/OUTLIER identique pour tous les champs BCM.

    Logique :
      field == 'NA'  ET  ref == 'NA'   → ('NA', 'NA')
      field == 'NA'  ET  ref != 'NA'   → ('OUTLIER', 'OUTLIER')
      field vide / null / NaN          → ('OUTLIER', 'OUTLIER')
      valeur non identifiée (OUTLIER)  → ('OUTLIER', 'OUTLIER')
      sinon                            → (current_iso, current_mth) inchangé

    Args:
        row       : ligne du DataFrame (pd.Series)
        field_col : nom de la colonne brute    (ex: "Devise", "ModeReglement")
        ref_col   : nom de la colonne ref      (ex: "ReferenceTransaction")
        iso_col   : nom de la colonne iso_out  (ex: "Devise_normalisee", "ModeReglement_normalisee")
        mth_col   : nom de la colonne method   (ex: "Devise_method", "ModeReglement_method")
    """
    field_upper = str(row.get(field_col, "")).strip().upper()
    ref_upper   = str(row.get(ref_col,   "")).strip().upper()
    current_iso = row[iso_col]
    current_mth = row[mth_col]

    if field_upper == "NA":
        return ("NA", "NA") if ref_upper == "NA" else ("OUTLIER", "OUTLIER")

    if field_upper in ("", "NAN", "NONE", "NULL"):
        return "OUTLIER", "OUTLIER"

    if current_iso == "OUTLIER":
        return "OUTLIER", "OUTLIER"

    return current_iso, current_mth


from shared.db_connector import load_table, load_file
from shared.writer import write_csv, write_excel_sheets


_BASE_CFG = Path(__file__).parent / "config_base.yaml"


def load_config(config_path: str | Path) -> dict:
    """
    Fusionne config_base.yaml + YAML spécifique.
    Les sections dict (input, output, llm, columns) sont fusionnées en profondeur.
    Le YAML spécifique surcharge uniquement les clés qu'il redéfinit.
    """
    with open(_BASE_CFG, encoding="utf-8") as f:
        base = yaml.safe_load(f)
    with open(config_path, encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    for k, v in spec.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = {**base[k], **v}
        else:
            base[k] = v
    return base


class BasePipeline:
    """Pipeline générique. Implémenter normalize() et build_output_tables()."""

    def normalize(self, df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
        raise NotImplementedError

    def build_output_tables(self, df: pd.DataFrame, cfg: dict) -> tuple:
        raise NotImplementedError

    def get_export_cols(self, df: pd.DataFrame, cfg: dict) -> list:
        """
        Colonnes pour le fichier extraction :
        toutes les colonnes originales + col_out juste après col_in.
        Les colonnes intermédiaires listées dans cfg[exclude_from_export] sont exclues.
        """
        col_in  = cfg["columns"]["field"]
        col_out = cfg["columns"]["field_out"]
        exclude = set(cfg.get("exclude_from_export", []))
        avail   = [c for c in df.columns if c not in exclude and c != col_out]
        idx     = avail.index(col_in) + 1 if col_in in avail else len(avail)
        return avail[:idx] + [col_out] + avail[idx:]

    def run(self, config_path: str | Path,
            override_input: Optional[str] = None,
            warm_start: bool = False) -> dict:
        """
        Pipeline complet pour une API.

        Nomenclature automatique des fichiers output :
            Source SQL Server   → tag -historique- dans le nom
            Source fichier local → pas de tag
        """
        cfg    = load_config(config_path)
        if warm_start:
            cfg["warm_start"] = True
        api_id = cfg["api_id"]
        inp    = cfg["input"]
        print(f"\n[{api_id}] Démarrage...")

        try:
            # 1. Chargement
            historique = (override_input is None
                          and inp.get("type", "sqlserver") == "sqlserver")
            if override_input:
                print(f"  [{api_id}] Source : fichier local → {override_input}")
                df_raw = load_file(override_input, cfg)
            else:
                table = inp["table_name"]
                field = cfg.get("columns", {}).get("field", "")
                print(f"  [{api_id}] Source : SQL Server → {table} (historique)")
                df_raw = load_table(table, field=field)
            print(f"  [{api_id}] {len(df_raw):,} lignes chargées")

            # 2. Normalisation
            df_out = self.normalize(df_raw, cfg)

            # 3. Stats
            col_out     = cfg["columns"]["field_out"]
            outlier_tag = cfg.get("outlier_tag", "OUTLIER")
            n           = len(df_out)
            n_ok        = (~df_out[col_out].isin([outlier_tag]) & df_out[col_out].notna()).sum()
            n_out       = (df_out[col_out] == outlier_tag).sum()
            
            # Stats warm-start (si colonne _method présente et warm_start actif)
            if cfg.get("warm_start", False):
                field_col = cfg["columns"]["field"]
                # Utiliser _ws_hit (flag avant apply_na_rule) si disponible
                if "_ws_hit" in df_out.columns:
                    warm_mask    = df_out["_ws_hit"] == True
                    cascade_mask = df_out["_ws_hit"] == False
                    # Exclure les NA/vides de la cascade (gérés par apply_na_rule, pas cascade réelle)
                    na_mask      = df_out[field_col].isna() | (df_out[field_col].astype(str).str.strip() == "")
                    cascade_mask = cascade_mask & ~na_mask
                else:
                    meth_cols    = [c for c in df_out.columns if c.endswith("_method")]
                    mc           = meth_cols[0] if meth_cols else None
                    warm_mask    = df_out[mc] == "WARM" if mc else pd.Series([False]*n)
                    cascade_mask = ~warm_mask

                n_warm_lig = int(warm_mask.sum())
                n_warm_mod = int(df_out.loc[warm_mask, field_col].nunique())
                n_new_lig  = int(cascade_mask.sum())
                n_new_mod  = int(df_out.loc[cascade_mask, field_col].nunique())

                print(f"  [{api_id}] Warm-start : {n_warm_mod} modalités uniques | {n_warm_lig:,} lignes")
                if 0 < n_new_mod <= 5:
                    new_vals = sorted(df_out.loc[cascade_mask, field_col].dropna().unique().tolist(),
                                      key=lambda x: str(x))
                    vals_str = " | ".join(str(v).strip() for v in new_vals)
                    print(f"  [{api_id}] Nouvelles→cascade : {n_new_mod} modalités | {n_new_lig:,} lignes → {vals_str}")
                elif n_new_mod > 5:
                    print(f"  [{api_id}] Nouvelles→cascade : {n_new_mod} modalités uniques | {n_new_lig:,} lignes")
                else:
                    print(f"  [{api_id}] Nouvelles→cascade : 0 — toutes les modalités connues")
            print(f"  [{api_id}] Résolues: {n_ok:,} | Outliers: {n_out:,} | Total: {n:,}")
            # 4. Outputs
            p_ext, p_map = self._save_outputs(df_out, cfg, historique)

            return {"api_id": api_id, "status": "OK", "historique": historique,
                    "n_rows": int(n), "n_resolved": int(n_ok), "n_outliers": int(n_out),
                    "paths": {"extraction": str(p_ext), "mapping": str(p_map)}}

        except Exception as exc:
            import traceback
            print(f"  [{api_id}] ERREUR : {exc}")
            traceback.print_exc()
            return {"api_id": api_id, "status": "ERROR", "error": str(exc)}

    def run_all(self, config_dir: str | Path, max_workers: int = 4,
                warm_start: bool = False) -> list:
        """Lance toutes les APIs du dossier config en parallèle."""
        paths = sorted(Path(config_dir).glob("*.yaml"))
        if not paths:
            print(f"Aucun .yaml dans {config_dir}"); return []

        print(f"\nLancement {len(paths)} APIs — {max_workers} workers parallèles...")
        results = []
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(self.run, str(p), None, warm_start): p.stem for p in paths}
            for f in as_completed(futures):
                try:    results.append(f.result())
                except Exception as e:
                    results.append({"api_id": futures[f], "status": "ERROR", "error": str(e)})

        print("\n══ RÉSUMÉ ══")
        for r in sorted(results, key=lambda x: x["api_id"]):
            if r["status"] == "OK":
                print(f"  {r['api_id']:20s} OK | {r['n_resolved']:>8,} résolues"
                      f" | {r['n_outliers']:>6,} outliers | {r['n_rows']:>8,} lignes")
            else:
                print(f"  {r['api_id']:20s} ERREUR : {r.get('error','')}")
        return results

    def _save_outputs(self, df: pd.DataFrame, cfg: dict, historique: bool):
        import os
        api_id   = cfg["api_id"]
        base_dir = os.getenv("OUTPUT_BASE", "")
        rel_dir  = cfg["output"]["dir"]
        out_dir  = Path(base_dir) / rel_dir if base_dir else Path(rel_dir)
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        tag     = f"{api_id}-historique" if historique else api_id
        out_dir.mkdir(parents=True, exist_ok=True)

        df_clean, df_analysis = self.build_output_tables(df, cfg)

        export = [c for c in self.get_export_cols(df, cfg) if c in df.columns]
        p_ext  = write_csv(df[export], out_dir / f"{tag}_extraction_{ts}.csv")
        p_map  = out_dir / f"{tag}_mapping_outliers_{ts}.xlsx"
        write_excel_sheets(
            {"Mapping_Clean": df_clean, "Analyse_Outliers": df_analysis},
            p_map,
            few_shot=getattr(self, "_few_shot", None),
        )

        print(f"  [{api_id}] Extraction  → {p_ext.name}")
        print(f"  [{api_id}] Mapping     → {p_map.name}")

        # Upload SharePoint (optionnel — activé si credentials définis dans .env)
        from shared.sharepoint_uploader import upload_file
        upload_file(p_ext, api_id=api_id)
        upload_file(p_map, api_id=api_id)

        return p_ext, p_map

    @classmethod
    def cli(cls, config_dir_default: str = "config/") -> None:
        """Interface CLI héritée par tous les pipelines."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--config",     help="YAML d'une API")
        parser.add_argument("--config-dir", default=config_dir_default,
                            help=f"Dossier YAMLs (défaut: {config_dir_default})")
        parser.add_argument("--all",        action="store_true",
                            help="Lancer toutes les APIs en parallèle")
        parser.add_argument("--input",      default=None,
                            help="Fichier local CSV/Excel (écrase SQL Server)")
        parser.add_argument("--workers",    type=int, default=4,
                            help="Workers parallèles (défaut: 4)")
        parser.add_argument("--schedule",   type=int, default=None,
                            help="Lancer automatiquement toutes les N jours (ex: --schedule 30)")
        parser.add_argument("--warm-start", action="store_true",
                            help="Utilise le cache validé pour les modalités connues, cascade pour les nouvelles")
        args     = parser.parse_args()
        pipeline = cls()

        # Injecter warm_start dans la config si demandé
        if getattr(args, "warm_start", False):
            from shared.base_pipeline import load_config as _orig
            def _patched(path):
                cfg = _orig(path)
                cfg["warm_start"] = True
                return cfg
            import shared.base_pipeline as _bp
            _bp.load_config = _patched

        def _run_once():
            if args.all:
                pipeline.run_all(args.config_dir, max_workers=args.workers)
            elif args.config:
                pipeline.run(args.config, override_input=args.input)
            else:
                parser.print_help(); sys.exit(1)

        if args.schedule:
            import time
            interval = args.schedule * 86400
            print(f"Mode planifié : exécution toutes les {args.schedule} jour(s). Ctrl+C pour arrêter.")
            while True:
                _run_once()
                print(f"  Prochaine exécution dans {args.schedule} jour(s).")
                time.sleep(interval)
        else:
            _run_once()