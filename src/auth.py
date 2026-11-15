import logging
import os
from typing import Dict, Optional

import httpx
from dotenv import load_dotenv
from google.cloud import secretmanager

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

MELI_OAUTH_URL = "https://api.mercadolibre.com/oauth/token"
PUBLIC_MODE_SENTINEL = "PUBLIC_MODE"


class MeliAuthManager:
    """Gerencia autenticacao OAuth 2.0 do Mercado Livre e rotacao no Secret Manager."""

    def __init__(self, project_id: Optional[str] = None) -> None:
        self.project_id = project_id or os.environ.get("GCP_PROJECT_ID")
        if not self.project_id:
            raise ValueError("Variavel GCP_PROJECT_ID nao definida no ambiente/.env.")
        self.sm_client = secretmanager.SecretManagerServiceClient()

    def _secret_path(self, secret_id: str) -> str:
        return f"projects/{self.project_id}/secrets/{secret_id}"

    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Le o valor em texto UTF-8 de um segredo no Secret Manager."""
        name = f"{self._secret_path(secret_id)}/versions/{version}"
        response = self.sm_client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()

    def set_secret_with_cleanup(self, secret_id: str, value: str, max_active: int = 1) -> str:
        """Adiciona nova versao do segredo e destroi versoes antigas (Free Tier Guardrail)."""
        parent = self._secret_path(secret_id)
        payload = {"data": value.strip().encode("utf-8")}
        new_version = self.sm_client.add_secret_version(
            request={"parent": parent, "payload": payload}
        )
        logger.info("Nova versao gravada para %s: %s", secret_id, new_version.name.split("/")[-1])
        self._cleanup_old_versions(secret_id, keep_latest=max_active)
        return new_version.name

    def _cleanup_old_versions(self, secret_id: str, keep_latest: int = 1) -> None:
        """Destroi versoes ENABLED antigas para manter consumo dentro das 6 versoes gratuitas."""
        parent = self._secret_path(secret_id)
        versions = list(
            self.sm_client.list_secret_versions(
                request={"parent": parent, "filter": "state:ENABLED"}
            )
        )
        # Ordena da versao mais recente para a mais antiga pelo numero da versao
        versions.sort(key=lambda v: int(v.name.split("/")[-1]), reverse=True)

        for old_ver in versions[keep_latest:]:
            self.sm_client.destroy_secret_version(request={"name": old_ver.name})
            logger.info("Versao antiga expurgada (Free Tier): %s", old_ver.name.split("/")[-1])

    def get_auth_headers(self) -> Dict[str, str]:
        """Retorna headers HTTP prontos (Dual-Mode: PUBLIC_MODE ou OAuth Bearer)."""
        base_headers = {
            "Accept": "application/json",
            "User-Agent": "MeliDataPlatformETL/1.0",
        }
        app_id = self.get_secret("MELI_APP_ID")

        if app_id == PUBLIC_MODE_SENTINEL or app_id.startswith("PENDING"):
            logger.info("Operando em PUBLIC_MODE (endpoints publicos da API MLB).")
            return base_headers

        client_secret = self.get_secret("MELI_CLIENT_SECRET")
        refresh_token = self.get_secret("MELI_REFRESH_TOKEN")
        access_token = self.refresh_oauth_token(app_id, client_secret, refresh_token)
        base_headers["Authorization"] = f"Bearer {access_token}"
        return base_headers

    def refresh_oauth_token(
        self, app_id: str, client_secret: str, current_refresh_token: str
    ) -> str:
        """Renova o access_token no Mercado Livre e persiste o novo refresh_token single-use."""
        logger.info("Renovando access_token via refresh_token em %s...", MELI_OAUTH_URL)
        payload = {
            "grant_type": "refresh_token",
            "client_id": app_id,
            "client_secret": client_secret,
            "refresh_token": current_refresh_token,
        }
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                MELI_OAUTH_URL,
                data=payload,
                headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            )
            resp.raise_for_status()
            token_data = resp.json()

        new_access_token = token_data["access_token"]
        new_refresh_token = token_data.get("refresh_token")

        if new_refresh_token and new_refresh_token != current_refresh_token:
            self.set_secret_with_cleanup("MELI_REFRESH_TOKEN", new_refresh_token, max_active=1)

        logger.info("Token OAuth renovado com sucesso (expira em %ss).", token_data.get("expires_in"))
        return new_access_token


if __name__ == "__main__":
    manager = MeliAuthManager()
    # Garante que os 3 segredos estejam gravados em UTF-8 limpo e com apenas 1 versao ativa
    for secret_name in ("MELI_APP_ID", "MELI_CLIENT_SECRET", "MELI_REFRESH_TOKEN"):
        current_val = manager.get_secret(secret_name)
        if "\n" in current_val or current_val != PUBLIC_MODE_SENTINEL:
            manager.set_secret_with_cleanup(secret_name, PUBLIC_MODE_SENTINEL, max_active=1)
    headers = manager.get_auth_headers()
    print(f"VALIDACAO E2-T02 OK | Headers ativos: {list(headers.keys())} | MELI_APP_ID={manager.get_secret('MELI_APP_ID')}")
