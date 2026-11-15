import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import yaml
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.auth import MeliAuthManager

logger = logging.getLogger(__name__)

MELI_BASE_URL = "https://api.mercadolibre.com"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class RetryableMeliError(Exception):
    """Excecao levantada apenas para erros HTTP transitorios (429 e 5xx)."""


class MeliClient:
    """Cliente HTTP resiliente para extracao de dados da API do Mercado Livre (MLB)."""

    def __init__(
        self,
        config_path: str = "config/categories.yaml",
        auth_manager: Optional[MeliAuthManager] = None,
    ) -> None:
        self.config = self._load_config(config_path)
        self.site_id: str = self.config.get("site_id", "MLB")
        self.auth_manager = auth_manager or MeliAuthManager()
        self.headers = self.auth_manager.get_auth_headers()
        self.is_oauth = "Authorization" in self.headers

    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        path = Path(config_path)
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    @staticmethod
    def compute_payload_hash(payload: Any) -> str:
        """Gera hash SHA-256 deterministico do payload JSON para controle de idempotencia."""
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @retry(
        retry=retry_if_exception_type((RetryableMeliError, httpx.TimeoutException, httpx.NetworkError)),
        wait=wait_random_exponential(multiplier=1.5, max=60),
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Executa GET em api.mercadolibre.com com Retry Exponencial + Full Jitter."""
        url = f"{MELI_BASE_URL}{endpoint}"
        with httpx.Client(timeout=15.0, headers=self.headers) as client:
            resp = client.get(url, params=params)
            if resp.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableMeliError(
                    f"HTTP {resp.status_code} transitorio em {endpoint}: {resp.text[:200]}"
                )
            resp.raise_for_status()
            return resp.json()

    def fetch_categories(self) -> List[Dict[str, Any]]:
        """Extrai detalhes, subcategorias e volume de anuncios ativos das categorias-alvo."""
        records: List[Dict[str, Any]] = []
        target_categories = self.config.get("target_categories", [])

        for cat in target_categories:
            cat_id = cat["id"]
            endpoint = f"/categories/{cat_id}"
            logger.info("Extraindo arvore e metricas da categoria %s (%s)...", cat_id, cat["name"])
            payload = self._get(endpoint)
            records.append(
                {
                    "endpoint": endpoint,
                    "category_id": cat_id,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload_hash": self.compute_payload_hash(payload),
                    "raw_payload": payload,
                }
            )
        return records

    def fetch_trends_and_domains(self) -> List[Dict[str, Any]]:
        """Extrai tendencias e descoberta de dominios/categorias folha para queries monitoradas."""
        records: List[Dict[str, Any]] = []
        queries: List[str] = self.config.get("tracked_product_queries", [])

        # 1. Domain Discovery (funciona tanto em PUBLIC_MODE quanto em OAUTH_MODE)
        endpoint_dd = f"/sites/{self.site_id}/domain_discovery/search"
        for query in queries:
            logger.info("Consultando domain_discovery para tendencia/produto: '%s'...", query)
            matches = self._get(endpoint_dd, params={"limit": 3, "q": query})
            top_cat_id = matches[0].get("category_id", "UNKNOWN") if matches else "UNKNOWN"
            wrapped_payload = {
                "query": query,
                "source": "domain_discovery",
                "matches": matches,
            }
            records.append(
                {
                    "endpoint": endpoint_dd,
                    "category_id": top_cat_id,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload_hash": self.compute_payload_hash(wrapped_payload),
                    "raw_payload": wrapped_payload,
                }
            )

        # 2. Se OAUTH_MODE estiver ativo, coleta tambem /trends/MLB/{category_id}
        if self.is_oauth:
            for cat in self.config.get("target_categories", []):
                cat_id = cat["id"]
                endpoint_tr = f"/trends/{self.site_id}/{cat_id}"
                try:
                    trends_payload = self._get(endpoint_tr)
                    records.append(
                        {
                            "endpoint": endpoint_tr,
                            "category_id": cat_id,
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                            "payload_hash": self.compute_payload_hash(trends_payload),
                            "raw_payload": {"source": "official_trends", "trends": trends_payload},
                        }
                    )
                except httpx.HTTPStatusError as exc:
                    logger.warning("Aviso ao consultar %s: %s", endpoint_tr, exc)

        return records


if __name__ == "__main__":
    client = MeliClient()
    cats = client.fetch_categories()
    trends = client.fetch_trends_and_domains()
    total_offers = sum(c["raw_payload"].get("total_items_in_this_category", 0) for c in cats)
    print(
        f"VALIDACAO E2-T03 OK | Categorias extraidas: {len(cats)} "
        f"(Total de ofertas monitoradas nas 5 macro-categorias: {total_offers:,}) | "
        f"Tendencias/Dominios extraidos: {len(trends)}"
    )
