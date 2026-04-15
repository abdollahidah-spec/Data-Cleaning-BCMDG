"""
shared/sharepoint_uploader.py
==============================
Upload des fichiers output vers SharePoint BCM Data Governance.
Optionnel — activé uniquement si les credentials SharePoint sont définis dans .env.

Prérequis :
    pip install msal requests

Variables .env requises :
    SHAREPOINT_TENANT_ID    — ex: bcm327.onmicrosoft.com (ou GUID)
    SHAREPOINT_CLIENT_ID    — App registration Azure AD
    SHAREPOINT_CLIENT_SECRET— Secret de l'app registration
    SHAREPOINT_SITE_URL     — ex: https://bcm327.sharepoint.com/sites/BCMDataGovernance
    SHAREPOINT_FOLDER_PATH  — ex: /Shared Documents/Data Quality/outputs
"""
from __future__ import annotations

import os
from pathlib import Path


def _is_configured() -> bool:
    """True si les 5 variables SharePoint sont définies dans l'environnement."""
    required = [
        "SHAREPOINT_TENANT_ID",
        "SHAREPOINT_CLIENT_ID",
        "SHAREPOINT_CLIENT_SECRET",
        "SHAREPOINT_SITE_URL",
        "SHAREPOINT_FOLDER_PATH",
    ]
    return all(os.getenv(k) for k in required)


def _get_access_token() -> str:
    """Obtient un token OAuth2 via MSAL (client credentials flow)."""
    import msal

    tenant   = os.getenv("SHAREPOINT_TENANT_ID")
    client   = os.getenv("SHAREPOINT_CLIENT_ID")
    secret   = os.getenv("SHAREPOINT_CLIENT_SECRET")
    authority = f"https://login.microsoftonline.com/{tenant}"

    app    = msal.ConfidentialClientApplication(client, secret, authority=authority)
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        raise RuntimeError(f"SharePoint auth échouée : {result.get('error_description')}")
    return result["access_token"]


def _get_drive_id(token: str, site_url: str) -> str:
    """Récupère le drive ID du site SharePoint via Microsoft Graph."""
    import requests

    # Extraire le hostname et le chemin du site
    from urllib.parse import urlparse
    parsed   = urlparse(site_url)
    hostname = parsed.netloc
    site_path = parsed.path.rstrip("/")

    # Obtenir le site ID
    site_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    site_resp.raise_for_status()
    site_id = site_resp.json()["id"]

    # Obtenir le drive par défaut
    drive_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    drive_resp.raise_for_status()
    return drive_resp.json()["id"]


def upload_file(local_path: str | Path, api_id: str = "") -> bool:
    """
    Upload un fichier local vers le dossier SharePoint configuré.

    Structure dans SharePoint :
        {SHAREPOINT_FOLDER_PATH}/{api_id}/{nom_du_fichier}

    Returns:
        True si upload réussi, False sinon (erreur loggée mais pas bloquante).
    """
    import requests

    if not _is_configured():
        return False  # SharePoint non configuré — pas d'erreur

    local_path = Path(local_path)
    if not local_path.exists():
        print(f"  [SharePoint] Fichier introuvable : {local_path}")
        return False

    try:
        token    = _get_access_token()
        site_url = os.getenv("SHAREPOINT_SITE_URL")
        folder   = os.getenv("SHAREPOINT_FOLDER_PATH", "/Shared Documents").rstrip("/")
        drive_id = _get_drive_id(token, site_url)

        # Chemin cible : folder/api_id/filename
        sub     = f"/{api_id}" if api_id else ""
        sp_path = f"{folder}{sub}/{local_path.name}"

        # Upload (PUT — crée ou écrase)
        with open(local_path, "rb") as f:
            data = f.read()

        upload_url = (
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
            f"/root:{sp_path}:/content"
        )
        resp = requests.put(
            upload_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type":  "application/octet-stream",
            },
            data=data,
            timeout=120,
        )
        resp.raise_for_status()
        print(f"  [SharePoint] Uploadé → {sp_path}")
        return True

    except Exception as e:
        print(f"  [SharePoint] Échec upload {local_path.name} : {e}")
        return False
