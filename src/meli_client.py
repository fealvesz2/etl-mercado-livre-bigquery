import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

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
MAX_MULTIGET_BATCH_SIZE = 20
DEFAULT_ITEM_ATTRIBUTES = (
    "id,title,family_name,user_product_id,category_id,price,base_price,"
    "original_price,currency_id,available_quantity,sold_quantity,"
    "condition,status,catalog_listing,shipping,permalink"
)


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
        configured_batch = int(self.config.get("multiget_batch_size", MAX_MULTIGET_BATCH_SIZE))
        self.multiget_batch_size = min(max(1, configured_batch), MAX_MULTIGET_BATCH_SIZE)
        self.search_limit = min(int(self.config.get("search_limit_per_category", 50)), 50)
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

    @staticmethod
    def chunk_ids(item_ids: Sequence[str], batch_size: int = MAX_MULTIGET_BATCH_SIZE) -> List[List[str]]:
        """Divide IDs em lotes de no maximo 20 itens (limite oficial do endpoint /items?ids=)."""
        effective_size = min(max(1, batch_size), MAX_MULTIGET_BATCH_SIZE)
        return [list(item_ids[i : i + effective_size]) for i in range(0, len(item_ids), effective_size)]

    def _build_record(self, endpoint: str, category_id: str, payload: Any) -> Dict[str, Any]:
        """Padroniza o registro de saida no contrato esperado pela camada Bronze."""
        return {
            "endpoint": endpoint,
            "category_id": category_id,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "payload_hash": self.compute_payload_hash(payload),
            "raw_payload": payload,
        }

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
        for cat in self.config.get("target_categories", []):
            cat_id = cat["id"]
            endpoint = f"/categories/{cat_id}"
            logger.info("Extraindo arvore e metricas da categoria %s (%s)...", cat_id, cat["name"])
            payload = self._get(endpoint)
            records.append(self._build_record(endpoint, cat_id, payload))
        return records

    def fetch_trends_and_domains(self) -> List[Dict[str, Any]]:
        """Extrai tendencias e descoberta de dominios/categorias folha para queries monitoradas."""
        records: List[Dict[str, Any]] = []
        queries: List[str] = self.config.get("tracked_product_queries", [])

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
            records.append(self._build_record(endpoint_dd, top_cat_id, wrapped_payload))

        if self.is_oauth:
            for cat in self.config.get("target_categories", []):
                cat_id = cat["id"]
                endpoint_tr = f"/trends/{self.site_id}/{cat_id}"
                try:
                    trends_payload = self._get(endpoint_tr)
                    records.append(
                        self._build_record(
                            endpoint_tr,
                            cat_id,
                            {"source": "official_trends", "trends": trends_payload},
                        )
                    )
                except httpx.HTTPStatusError as exc:
                    logger.warning("Aviso ao consultar %s: %s", endpoint_tr, exc)

        return records

    def fetch_highlights(self, category_id: str) -> List[str]:
        """Extrai IDs dos top 20 mais vendidos na categoria (requer OAUTH_MODE)."""
        endpoint = f"/highlights/{self.site_id}/category/{category_id}"
        payload = self._get(endpoint)
        return [
            item["id"]
            for item in payload.get("content", [])
            if item.get("type") == "ITEM" and item.get("id")
        ]

    def fetch_search_item_ids(self, category_id: str) -> List[str]:
        """Busca IDs de anuncios ativos na categoria respeitando o limite amostral (requer OAUTH_MODE)."""
        endpoint = f"/sites/{self.site_id}/search"
        payload = self._get(endpoint, params={"category": category_id, "limit": self.search_limit})
        return [item["id"] for item in payload.get("results", []) if item.get("id")]

    def fetch_items_multiget(
        self, item_ids: Iterable[str], attributes: str = DEFAULT_ITEM_ATTRIBUTES
    ) -> List[Dict[str, Any]]:
        """Enriquece detalhes de anuncios em lotes de ate 20 IDs via /items?ids=... (requer OAUTH_MODE)."""
        unique_ids = list(dict.fromkeys(item_ids))
        records: List[Dict[str, Any]] = []

        for batch in self.chunk_ids(unique_ids, self.multiget_batch_size):
            endpoint = "/items"
            logger.info("Consultando Multiget para lote de %d itens...", len(batch))
            responses = self._get(
                endpoint,
                params={"ids": ",".join(batch), "attributes": attributes},
            )
            for entry in responses:
                if entry.get("code") == 200 and isinstance(entry.get("body"), dict):
                    body = entry["body"]
                    records.append(
                        self._build_record(
                            endpoint="/items?ids=multiget",
                            category_id=body.get("category_id", "UNKNOWN"),
                            payload={"source": "items_multiget", "item": body},
                        )
                    )
                else:
                    logger.warning(
                        "Item ignorado no Multiget (status=%s): %s",
                        entry.get("code"),
                        entry.get("body"),
                    )
        return records

    def fetch_leaf_enrichment(
        self,
        category_records: List[Dict[str, Any]],
        trend_records: List[Dict[str, Any]],
        max_children_per_macro: int = 2,
    ) -> List[Dict[str, Any]]:
        """Em PUBLIC_MODE, coleta metricas e ficha tecnica das categorias-folha descobertas."""
        leaf_ids: List[str] = []

        for trend in trend_records:
            cat_id = trend.get("category_id")
            if cat_id and cat_id != "UNKNOWN":
                leaf_ids.append(cat_id)

        for cat_rec in category_records:
            children = cat_rec.get("raw_payload", {}).get("children_categories", [])
            top_children = sorted(
                children,
                key=lambda c: c.get("total_items_in_this_category", 0),
                reverse=True,
            )[:max_children_per_macro]
            for child in top_children:
                if child.get("id"):
                    leaf_ids.append(child["id"])

        unique_leaf_ids = list(dict.fromkeys(leaf_ids))
        records: List[Dict[str, Any]] = []

        for leaf_id in unique_leaf_ids:
            cat_endpoint = f"/categories/{leaf_id}"
            attr_endpoint = f"/categories/{leaf_id}/attributes"
            logger.info("Enriquecendo nicho/categoria folha %s (metricas + ficha tecnica)...", leaf_id)

            cat_detail = self._get(cat_endpoint)
            attrs = self._get(attr_endpoint)
            required_attrs = [
                {
                    "id": a.get("id"),
                    "name": a.get("name"),
                    "value_type": a.get("value_type"),
                    "required": bool(a.get("tags", {}).get("required")),
                    "catalog_required": bool(a.get("tags", {}).get("catalog_required")),
                    "allow_variations": bool(a.get("tags", {}).get("allow_variations")),
                }
                for a in attrs
                if a.get("tags", {}).get("required")
                or a.get("tags", {}).get("catalog_required")
                or a.get("tags", {}).get("allow_variations")
                or a.get("id") == "GTIN"
            ]

            snapshot_payload = {
                "source": "public_leaf_enrichment",
                "category_id": leaf_id,
                "category_name": cat_detail.get("name"),
                "total_items_in_this_category": cat_detail.get("total_items_in_this_category", 0),
                "is_leaf": len(cat_detail.get("children_categories", [])) == 0,
                "path_from_root": cat_detail.get("path_from_root", []),
                "settings": cat_detail.get("settings", {}),
                "total_attributes_count": len(attrs),
                "key_attributes": required_attrs,
            }
            records.append(self._build_record(cat_endpoint, leaf_id, snapshot_payload))

        return records

    def fetch_items_or_niches_snapshot(
        self,
        category_records: List[Dict[str, Any]],
        trend_records: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Orquestra extracao de itens (OAUTH_MODE) ou enriquecimento de nichos (PUBLIC_MODE)."""
        if not self.is_oauth:
            logger.info("PUBLIC_MODE ativo: executando enriquecimento de nichos e atributos de catalogo.")
            return self.fetch_leaf_enrichment(category_records, trend_records)

        collected_ids: List[str] = []
        for cat in self.config.get("target_categories", []):
            cat_id = cat["id"]
            try:
                collected_ids.extend(self.fetch_highlights(cat_id))
            except httpx.HTTPStatusError as exc:
                logger.warning("Falha em /highlights/%s: %s", cat_id, exc)
            try:
                collected_ids.extend(self.fetch_search_item_ids(cat_id))
            except httpx.HTTPStatusError as exc:
                logger.warning("Falha em /search para %s: %s", cat_id, exc)

        return self.fetch_items_multiget(collected_ids)


if __name__ == "__main__":
    client = MeliClient()
    cats = client.fetch_categories()
    trends = client.fetch_trends_and_domains()
    details = client.fetch_items_or_niches_snapshot(cats, trends)
    total_offers = sum(c["raw_payload"].get("total_items_in_this_category", 0) for c in cats)
    print(
        f"VALIDACAO E2-T04 OK | Modo: {'OAUTH' if client.is_oauth else 'PUBLIC'} | "
        f"Macro-categorias: {len(cats)} ({total_offers:,} ofertas) | "
        f"Tendencias/Dominios: {len(trends)} | Nichos/Itens enriquecidos: {len(details)}"
    )
