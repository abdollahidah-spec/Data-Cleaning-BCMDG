@echo off
REM ============================================================
REM  BCM Data Cleaning Pipeline — Lancement automatique
REM  Configurer dans Windows Task Scheduler pour execution
REM  automatique (hebdomadaire, mensuelle, etc.)  REM Activer l'environnement conda call conda activate data_clean

REM ============================================================

REM Aller a la racine du projet
cd /d "%~dp0"

echo.
echo [%date% %time%] Demarrage BCM Data Cleaning Pipeline
echo ============================================================

REM Activer l'environnement 
conda call conda activate data_clean

REM ── Devise (5 APIs) ──────────────────────────────────────────
echo [Devise] Lancement avec warm-start...
python devise/pipeline_devise.py --all --config-dir devise/config/ --warm-start

REM ── Pays (3 APIs) ────────────────────────────────────────────
echo [Pays] Lancement avec warm-start...
python pays/pipeline_pays.py --all --config-dir pays/config/ --warm-start

REM ── TypeSwift (2 APIs) ───────────────────────────────────────
echo [TypeSwift] Lancement avec warm-start...
python typeswift/pipeline_typeswift.py --all --config-dir typeswift/config/ --warm-start

REM ── ModeReglement (2 API) ────────────────────────────────────
echo [ModeReglement] Lancement avec warm-start...
python mode_reglement/pipeline_mode_reglement.py --all --config-dir mode_reglement/config/ --warm-start

REM ── NatureEconomique (2 APIs) ────────────────────────────────
REM Utilise --warm-start pour optimiser (cache v1 + cascade pour nouvelles modalites)
echo [NatureEconomique] Lancement avec warm-start...
python nature_economique/pipeline_nature_economique.py --all --config-dir nature_economique/config/ --warm-start

echo.
echo [%date% %time%] Pipeline termine.
echo ============================================================
pause
