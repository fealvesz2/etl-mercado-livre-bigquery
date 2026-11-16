import gc
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
import yaml
from tenacity import (
    RetryCallState,
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
DEFAULT_REQUEST_INTERVAL_MS = 80
DEFAULT_ITEM_ATTRIBUTES = (
    "id,title,family_name,user_product_id,category_id,price,base_price,"
    "original_price,currency_id,available_quantity,sold_quantity,"
    "condition,status,catalog_listing,shipping,permalink"
)
_DEFAULT_EXPONENTIAL_WAIT = wait_random_exponential(multiplier=1.5, max=60)


class RetryableMeliError(Exception):
    """Excecao levantada apenas para erros HTTP transitorios (429 e 5xx)."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


def _parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    """Converte header Retry-After (segundos ou HTTP-date) para segundos limitados a 60s."""
    if not header_value:
        return None
    raw = header_value.strip()
    try:
        return min(max(0.0, float(raw)), 60.0)
    except ValueError:
        pass
    try:
        retry_dt = parsedate_to_datetime(raw)
        if retry_dt.tzinfo is None:
            retry_dt = retry_dt.replace(tzinfo=timezone.utc)
        delta = (retry_dt - datetime.now(timezone.utc)).total_seconds()
        return min(max(0.0, delta), 60.0)
    except (TypeError, ValueError, OverflowError):
        return None


def _wait_retry_after_or_exponential(retry_state: RetryCallState) -> float:
    """Prioriza o header Retry-After em respostas 429; usa backoff exponencial caso contrario."""
    outcome = retry_state.outcome
    if outcome is not None and outcome.failed:
        exc = outcome.exception()
        if isinstance(exc, RetryableMeliError) and exc.retry_after is not None:
            return exc.retry_after
    return _DEFAULT_EXPONENTIAL_WAIT(retry_state)


class MeliClient:
    """Cliente HTTP resiliente para extracao de dados da API do Mercado Livre (MLB)."""

    def __init__(
        self,
        config_path: str = "config/categories.yaml",
        auth_manager: Optional[MeliAuthManager] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        self.config = self._load_config(config_path)
        self.site_id: str = self.config.get("site_id", "MLB")
        configured_batch = int(self.config.get("multiget_batch_size", MAX_MULTIGET_BATCH_SIZE))
        self.multiget_batch_size = min(max(1, configured_batch), MAX_MULTIGET_BATCH_SIZE)
        self.search_limit = min(int(self.config.get("search_limit_per_category", 50)), 50)
        self.request_interval_s = max(
            0.0, float(self.config.get("request_interval_ms", DEFAULT_REQUEST_INTERVAL_MS)) / 1000.0
        )

        leaf_cfg = self.config.get("leaf_discovery", {})
        self.use_bulk_tree: bool = bool(leaf_cfg.get("use_bulk_tree", True))
        self.include_all_macros_summary: bool = bool(leaf_cfg.get("include_all_macros_summary", True))
        self.max_depth: int = max(1, int(leaf_cfg.get("max_depth", 7)))
        self.max_children_per_node: int = max(1, int(leaf_cfg.get("max_children_per_node", 4)))
        self.top_leaves_per_macro: int = max(1, int(leaf_cfg.get("top_leaves_per_macro", 10)))

        self.auth_manager = auth_manager or MeliAuthManager()
        self.headers = self.auth_manager.get_auth_headers()
        self.is_oauth = "Authorization" in self.headers

        self._owns_http_client = http_client is None
        self._http_client: Optional[httpx.Client] = http_client
        self._last_request_ts: Optional[float] = None
        self._category_cache: Dict[str, Dict[str, Any]] = {}
        self._preselected_leaves_by_macro: Dict[str, List[str]] = {}

    def __enter__(self) -> "MeliClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Encerra a sessao HTTP persistente quando criada pela propria instancia."""
        if self._owns_http_client and self._http_client is not None:
            self._http_client.close()
            self._http_client = None

    def _get_http_client(self) -> httpx.Client:
        """Retorna cliente HTTP persistente com reaproveitamento de conexoes TCP/TLS."""
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=30.0, headers=self.headers)
        return self._http_client

    def _apply_pacing(self) -> None:
        """Aplica intervalo minimo configuravel entre requisicoes sequenciais anti-429."""
        if self.request_interval_s <= 0:
            return
        now = time.monotonic()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            if elapsed < self.request_interval_s:
                time.sleep(self.request_interval_s - elapsed)
        self._last_request_ts = time.monotonic()

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

    @staticmethod
    def _strip_translations(cat_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Remove bloco multilingue 'translations' para reduzir uso de RAM e JSON na Bronze."""
        if "translations" not in cat_payload:
            return dict(cat_payload)
        return {k: v for k, v in cat_payload.items() if k != "translations"}

    def _build_record(self, endpoint: str, category_id: Optional[str], payload: Any) -> Dict[str, Any]:
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
        wait=_wait_retry_after_or_exponential,
        stop=stop_after_attempt(4),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Executa GET em api.mercadolibre.com com Pacing, Retry-After e Jitter."""
        url = f"{MELI_BASE_URL}{endpoint}"
        self._apply_pacing()
        client = self._get_http_client()
        resp = client.get(url, params=params)
        if resp.status_code in RETRYABLE_STATUS_CODES:
            retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
            raise RetryableMeliError(
                f"HTTP {resp.status_code} transitorio em {endpoint}: {resp.text[:200]}",
                status_code=resp.status_code,
                retry_after=retry_after,
            )
        resp.raise_for_status()
        return resp.json()

    def _get_category_detail(self, category_id: str) -> Dict[str, Any]:
        """Consulta detalhe de categoria reutilizando cache em memoria da execucao."""
        cached = self._category_cache.get(category_id)
        if cached is not None:
            return cached
        detail = self._strip_translations(self._get(f"/categories/{category_id}"))
        self._category_cache[category_id] = detail
        return detail

    def _select_leaves_from_bulk_tree(
        self,
        all_cats: Dict[str, Dict[str, Any]],
        macro_id: str,
        top_leaves: int,
    ) -> List[str]:
        """Percorre subarvore no bulk em memoria ate folhas reais e retorna top-N por volume."""
        leaves: List[Dict[str, Any]] = []
        stack: List[Tuple[str, int]] = [(macro_id, 1)]
        visited = set()

        while stack:
            cid, depth = stack.pop()
            if cid in visited or depth > self.max_depth:
                continue
            visited.add(cid)
            node = all_cats.get(cid)
            if not isinstance(node, dict):
                continue

            children = node.get("children_categories") or []
            if not children:
                leaves.append(node)
                continue

            if depth < self.max_depth:
                for child in children:
                    child_id = child.get("id")
                    if child_id and child_id not in visited:
                        stack.append((child_id, depth + 1))

        leaves.sort(key=lambda c: c.get("total_items_in_this_category", 0), reverse=True)
        selected_ids: List[str] = []
        for leaf in leaves[:top_leaves]:
            leaf_id = leaf.get("id")
            if leaf_id:
                self._category_cache[leaf_id] = self._strip_translations(leaf)
                selected_ids.append(leaf_id)
        return selected_ids

    def _discover_leaves_via_api(
        self,
        macro_id: str,
        top_leaves: int,
        max_children_per_node: int,
    ) -> List[str]:
        """Fallback BFS recursivo via /categories/{id} ate folhas reais (is_leaf=true)."""
        leaves: List[Dict[str, Any]] = []
        queue: List[Tuple[str, int]] = [(macro_id, 1)]
        visited = set()

        while queue:
            cid, depth = queue.pop(0)
            if cid in visited or depth > self.max_depth:
                continue
            visited.add(cid)

            detail = self._get_category_detail(cid)
            children = detail.get("children_categories") or []
            if not children:
                leaves.append(detail)
                continue

            if depth < self.max_depth:
                ranked_children = sorted(
                    children,
                    key=lambda c: c.get("total_items_in_this_category", 0),
                    reverse=True,
                )[:max_children_per_node]
                for child in ranked_children:
                    child_id = child.get("id")
                    if child_id and child_id not in visited:
                        queue.append((child_id, depth + 1))

        leaves.sort(key=lambda c: c.get("total_items_in_this_category", 0), reverse=True)
        return [leaf["id"] for leaf in leaves[:top_leaves] if leaf.get("id")]

    def fetch_categories(self) -> List[Dict[str, Any]]:
        """Extrai arvores das macro-categorias via bulk tree (/categories/all) com fallback recursivo."""
        self._category_cache.clear()
        self._preselected_leaves_by_macro.clear()
        target_cats = self.config.get("target_categories", [])
        target_ids = {cat["id"] for cat in target_cats if cat.get("id")}

        if self.use_bulk_tree:
            bulk_endpoint = f"/sites/{self.site_id}/categories/all"
            try:
                logger.info("Carregando arvore publica completa via %s...", bulk_endpoint)
                all_cats = self._get(bulk_endpoint)
                if isinstance(all_cats, dict) and all_cats:
                    records: List[Dict[str, Any]] = []
                    macro_ids_to_emit = set(target_ids)
                    if self.include_all_macros_summary:
                        for cid, cat in all_cats.items():
                            if isinstance(cat, dict) and len(cat.get("path_from_root", [])) == 1:
                                macro_ids_to_emit.add(cid)

                    for cid in sorted(
                        macro_ids_to_emit,
                        key=lambda x: all_cats.get(x, {}).get("total_items_in_this_category", 0),
                        reverse=True,
                    ):
                        raw_cat = all_cats.get(cid)
                        if not isinstance(raw_cat, dict):
                            continue
                        slim_cat = self._strip_translations(raw_cat)
                        self._category_cache[cid] = slim_cat
                        records.append(self._build_record(f"/categories/{cid}", cid, slim_cat))

                    for cat in target_cats:
                        cid = cat.get("id")
                        if cid and cid in all_cats:
                            self._preselected_leaves_by_macro[cid] = self._select_leaves_from_bulk_tree(
                                all_cats, cid, top_leaves=self.top_leaves_per_macro
                            )

                    del all_cats
                    gc.collect()
                    logger.info(
                        "Arvore bulk processada: %d macros emitidas, %d macros com folhas pre-selecionadas.",
                        len(records),
                        len(self._preselected_leaves_by_macro),
                    )
                    return records
            except (httpx.HTTPError, RetryableMeliError, ValueError) as exc:
                logger.warning("Fallback para /categories/{id} apos falha em %s: %s", bulk_endpoint, exc)

        records = []
        for cat in target_cats:
            cat_id = cat["id"]
            endpoint = f"/categories/{cat_id}"
            logger.info("Extraindo arvore e metricas da categoria %s (%s)...", cat_id, cat["name"])
            payload = self._get_category_detail(cat_id)
            records.append(self._build_record(endpoint, cat_id, payload))
        return records

    def fetch_trends_and_domains(self) -> List[Dict[str, Any]]:
        """Extrai tendencias e descoberta de dominios/categorias (ranks 1-3) para queries monitoradas."""
        records: List[Dict[str, Any]] = []
        queries: List[str] = self.config.get("tracked_product_queries", [])

        endpoint_dd = f"/sites/{self.site_id}/domain_discovery/search"
        for query in queries:
            logger.info("Consultando domain_discovery para tendencia/produto: '%s'...", query)
            matches = self._get(endpoint_dd, params={"limit": 3, "q": query})
            top_cat_id = matches[0].get("category_id", "UNKNOWN") if matches else "UNKNOWN"
            matched_ids = [
                m["category_id"]
                for m in matches
                if isinstance(m, dict) and m.get("category_id")
            ]
            wrapped_payload = {
                "query": query,
                "source": "domain_discovery",
                "matched_category_ids": matched_ids,
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

    @staticmethod
    def _extract_discovery_category_ids(trend_records: Sequence[Dict[str, Any]]) -> List[str]:
        """Coleta todos os category_id retornados nos ranks 1, 2 e 3 do domain_discovery."""
        category_ids: List[str] = []
        for trend in trend_records:
            matches = trend.get("raw_payload", {}).get("matches")
            if isinstance(matches, list) and matches:
                for match in matches:
                    if isinstance(match, dict):
                        cid = match.get("category_id")
                        if cid and cid != "UNKNOWN":
                            category_ids.append(cid)
            else:
                cid = trend.get("category_id")
                if cid and cid != "UNKNOWN":
                    category_ids.append(cid)
        return list(dict.fromkeys(category_ids))

    @staticmethod
    def _extract_key_attributes(attrs: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """Extrai atributos-chave e contadores resumidos da ficha tecnica da categoria."""
        summary = {
            "total": len(attrs),
            "required": 0,
            "catalog_required": 0,
            "conditional_required": 0,
            "variation_attributes": 0,
            "defines_picture": 0,
            "with_preset_values": 0,
        }
        key_attrs: List[Dict[str, Any]] = []

        for attr in attrs:
            tags = attr.get("tags") or {}
            is_req = bool(tags.get("required"))
            is_cat_req = bool(tags.get("catalog_required"))
            is_cond_req = bool(tags.get("conditional_required"))
            is_allow_var = bool(tags.get("allow_variations"))
            is_var_attr = bool(tags.get("variation_attribute"))
            is_def_pic = bool(tags.get("defines_picture"))

            summary["required"] += int(is_req)
            summary["catalog_required"] += int(is_cat_req)
            summary["conditional_required"] += int(is_cond_req)
            summary["variation_attributes"] += int(is_allow_var or is_var_attr)
            summary["defines_picture"] += int(is_def_pic)
            summary["with_preset_values"] += int(bool(attr.get("values")))

            if is_req or is_cat_req or is_cond_req or is_allow_var or is_var_attr or attr.get("id") == "GTIN":
                key_attrs.append(
                    {
                        "id": attr.get("id"),
                        "name": attr.get("name"),
                        "value_type": attr.get("value_type"),
                        "relevance": attr.get("relevance"),
                        "hierarchy": attr.get("hierarchy"),
                        "attribute_group_name": attr.get("attribute_group_name"),
                        "required": is_req,
                        "catalog_required": is_cat_req,
                        "conditional_required": is_cond_req,
                        "allow_variations": is_allow_var,
                        "variation_attribute": is_var_attr,
                    }
                )

        return key_attrs, summary

    def fetch_leaf_enrichment(
        self,
        category_records: List[Dict[str, Any]],
        trend_records: List[Dict[str, Any]],
        max_children_per_macro: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Em PUBLIC_MODE, coleta metricas e ficha tecnica de ranks 1-3 e folhas reais."""
        leaf_ids: List[str] = self._extract_discovery_category_ids(trend_records)
        top_leaves = max_children_per_macro if max_children_per_macro is not None else self.top_leaves_per_macro
        target_macro_ids = {cat.get("id") for cat in self.config.get("target_categories", []) if cat.get("id")}

        for cat_rec in category_records:
            raw_cat = cat_rec.get("raw_payload", {})
            macro_id = raw_cat.get("id") or cat_rec.get("category_id")
            if not macro_id:
                continue
            if target_macro_ids and macro_id not in target_macro_ids:
                continue

            self._category_cache.setdefault(macro_id, self._strip_translations(raw_cat))
            if macro_id in self._preselected_leaves_by_macro and max_children_per_macro is None:
                leaf_ids.extend(self._preselected_leaves_by_macro[macro_id])
            else:
                leaf_ids.extend(
                    self._discover_leaves_via_api(
                        macro_id,
                        top_leaves=top_leaves,
                        max_children_per_node=self.max_children_per_node,
                    )
                )

        unique_leaf_ids = list(dict.fromkeys(leaf_ids))
        records: List[Dict[str, Any]] = []
        logger.info("Iniciando enriquecimento tecnico para %d nichos/folhas unicos...", len(unique_leaf_ids))

        for leaf_id in unique_leaf_ids:
            cat_endpoint = f"/categories/{leaf_id}"
            attr_endpoint = f"/categories/{leaf_id}/attributes"

            cat_detail = self._get_category_detail(leaf_id)
            attrs = self._get(attr_endpoint)
            if not isinstance(attrs, list):
                attrs = []
            key_attrs, attr_summary = self._extract_key_attributes(attrs)
            children = cat_detail.get("children_categories") or []
            path_from_root = cat_detail.get("path_from_root") or []

            snapshot_payload = {
                "source": "public_leaf_enrichment",
                "category_id": leaf_id,
                "category_name": cat_detail.get("name"),
                "permalink": cat_detail.get("permalink"),
                "total_items_in_this_category": cat_detail.get("total_items_in_this_category", 0),
                "children_count": len(children),
                "is_leaf": len(children) == 0,
                "category_depth": len(path_from_root),
                "path_from_root": path_from_root,
                "settings": cat_detail.get("settings", {}),
                "channels_settings": cat_detail.get("channels_settings", []),
                "total_attributes_count": attr_summary["total"],
                "attribute_summary": attr_summary,
                "key_attributes": key_attrs,
            }
            records.append(self._build_record(cat_endpoint, leaf_id, snapshot_payload))

        return records

    def fetch_payment_methods(self) -> List[Dict[str, Any]]:
        """Extrai meios de pagamento e faixas de parcelamento (payer_costs) do MLB."""
        list_endpoint = f"/sites/{self.site_id}/payment_methods"
        logger.info("Extraindo catalogo publico de meios de pagamento (%s)...", list_endpoint)
        methods = self._get(list_endpoint)
        if not isinstance(methods, list):
            return []

        payment_types_by_id: Dict[str, List[str]] = {}
        unique_methods: Dict[str, Dict[str, Any]] = {}
        for method in methods:
            if not isinstance(method, dict) or not method.get("id"):
                continue
            pm_id = str(method["id"])
            ptype = method.get("payment_type_id")
            types_list = payment_types_by_id.setdefault(pm_id, [])
            if ptype and ptype not in types_list:
                types_list.append(str(ptype))
            unique_methods.setdefault(pm_id, method)

        currency_ref: Optional[Dict[str, Any]] = None
        if self.is_oauth:
            try:
                currency_ref = self._get("/currencies/BRL")
            except (httpx.HTTPError, RetryableMeliError) as exc:
                logger.warning("Aviso ao consultar /currencies/BRL: %s", exc)

        records: List[Dict[str, Any]] = []
        for pm_id, method in unique_methods.items():
            detail_endpoint = f"/sites/{self.site_id}/payment_methods/{pm_id}"
            try:
                detail = self._get(detail_endpoint)
            except (httpx.HTTPError, RetryableMeliError) as exc:
                logger.warning("Fallback para resumo de %s apos erro: %s", detail_endpoint, exc)
                detail = method

            records.append(
                self._build_record(
                    detail_endpoint,
                    pm_id,
                    {
                        "source": "payment_method_detail",
                        "supported_payment_types": payment_types_by_id.get(pm_id, []),
                        "currency_reference": currency_ref or {"id": "BRL"},
                        "payment_method": detail,
                    },
                )
            )
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
    with MeliClient() as client:
        cats = client.fetch_categories()
        trends = client.fetch_trends_and_domains()
        details = client.fetch_items_or_niches_snapshot(cats, trends)
        payments = client.fetch_payment_methods()
        total_offers = sum(c["raw_payload"].get("total_items_in_this_category", 0) for c in cats)
        leaf_count = sum(1 for d in details if d["raw_payload"].get("is_leaf"))
        print(
            f"VALIDACAO E6 OK | Modo: {'OAUTH' if client.is_oauth else 'PUBLIC'} | "
            f"Macro-categorias: {len(cats)} ({total_offers:,} ofertas) | "
            f"Tendencias/Dominios: {len(trends)} | Nichos/Itens: {len(details)} (folhas={leaf_count}) | "
            f"Meios pagamento: {len(payments)}"
        )
