import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import yaml

from src.meli_client import MeliClient, RetryableMeliError, _parse_retry_after


class TestMeliClient(unittest.TestCase):
    def _build_client(self, is_oauth: bool, config_override: dict | None = None) -> MeliClient:
        mock_auth = MagicMock()
        mock_auth.get_auth_headers.return_value = (
            {"Accept": "application/json", "Authorization": "Bearer APP_USR-TEST"}
            if is_oauth
            else {"Accept": "application/json"}
        )
        if config_override is None:
            client = MeliClient(config_path="config/categories.yaml", auth_manager=mock_auth)
            client.request_interval_s = 0.0
            return client

        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.safe_dump(config_override, tmp)
            tmp_path = Path(tmp.name)
        try:
            return MeliClient(config_path=str(tmp_path), auth_manager=mock_auth)
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_chunk_ids_respects_max_batch_of_20(self) -> None:
        ids = [f"MLB{i}" for i in range(45)]
        chunks = MeliClient.chunk_ids(ids, batch_size=25)
        self.assertEqual([len(c) for c in chunks], [20, 20, 5])

    def test_multiget_batches_and_filters_failed_items(self) -> None:
        client = self._build_client(is_oauth=True)
        ids = [f"MLB{i}" for i in range(25)]

        with patch.object(client, "_get") as mock_get:
            mock_get.side_effect = [
                [{"code": 200, "body": {"id": f"MLB{i}", "category_id": "MLB1055", "price": 100}} for i in range(20)],
                [
                    {"code": 200, "body": {"id": "MLB20", "category_id": "MLB1055", "price": 200}},
                    {"code": 404, "body": {"id": "MLB21", "message": "not found"}},
                ],
            ]
            records = client.fetch_items_multiget(ids)

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(len(records), 21)
        self.assertEqual(records[0]["endpoint"], "/items?ids=multiget")
        self.assertEqual(records[0]["raw_payload"]["source"], "items_multiget")

    def test_retry_on_429_respects_retry_after_and_fail_fast_on_400(self) -> None:
        self.assertEqual(_parse_retry_after("3"), 3.0)
        self.assertEqual(_parse_retry_after("999"), 60.0)
        self.assertIsNone(_parse_retry_after("invalid-date"))

        client = self._build_client(is_oauth=False)
        sleep_calls: list[float] = []
        client._get.retry.sleep = sleep_calls.append

        req = httpx.Request("GET", "https://api.mercadolibre.com/categories/MLB1051")
        resp_429 = httpx.Response(429, request=req, headers={"Retry-After": "2"}, text="rate limit")
        resp_200 = httpx.Response(200, request=req, json={"id": "MLB1051"})
        resp_400 = httpx.Response(400, request=req, text="bad request")

        mock_http = MagicMock()
        mock_http.get.side_effect = [resp_429, resp_200]
        client._http_client = mock_http

        data = client._get("/categories/MLB1051")
        self.assertEqual(data["id"], "MLB1051")
        self.assertEqual(mock_http.get.call_count, 2)
        self.assertEqual(sleep_calls, [2.0])

        mock_http.reset_mock()
        mock_http.get.side_effect = None
        mock_http.get.return_value = resp_400
        with self.assertRaises(httpx.HTTPStatusError):
            client._get("/categories/INVALID")
        self.assertEqual(mock_http.get.call_count, 1)

    def test_fetch_categories_bulk_tree_strips_translations_and_selects_true_leaves(self) -> None:
        cfg = {
            "site_id": "MLB",
            "request_interval_ms": 0,
            "leaf_discovery": {
                "use_bulk_tree": True,
                "include_all_macros_summary": True,
                "max_depth": 4,
                "max_children_per_node": 2,
                "top_leaves_per_macro": 1,
            },
            "target_categories": [{"id": "MLB1051", "name": "Celulares"}],
            "tracked_product_queries": ["Smartphone Samsung"],
        }
        client = self._build_client(is_oauth=False, config_override=cfg)

        bulk_tree = {
            "MLB1051": {
                "id": "MLB1051",
                "name": "Celulares",
                "permalink": "https://lista.mercadolivre.com.br/celulares",
                "total_items_in_this_category": 1000,
                "path_from_root": [{"id": "MLB1051", "name": "Celulares"}],
                "children_categories": [{"id": "MLB3813", "total_items_in_this_category": 800}],
                "settings": {"shipping_options": ["carrier", "custom"]},
                "translations": {"pt": "Celulares"},
            },
            "MLB3813": {
                "id": "MLB3813",
                "name": "Acessorios",
                "total_items_in_this_category": 800,
                "path_from_root": [{"id": "MLB1051"}, {"id": "MLB3813"}],
                "children_categories": [
                    {"id": "MLB1055", "total_items_in_this_category": 500},
                    {"id": "MLB9999", "total_items_in_this_category": 100},
                ],
                "settings": {"shipping_options": ["custom", "carrier"]},
            },
            "MLB1055": {
                "id": "MLB1055",
                "name": "Smartphones",
                "permalink": "https://lista.mercadolivre.com.br/smartphones",
                "total_items_in_this_category": 500,
                "path_from_root": [{"id": "MLB1051"}, {"id": "MLB3813"}, {"id": "MLB1055"}],
                "children_categories": [],
                "settings": {"shipping_options": ["carrier", "custom"], "maximum_price": 99999},
                "translations": {"pt": "Smartphones"},
            },
            "MLB9999": {
                "id": "MLB9999",
                "name": "Capinhas",
                "total_items_in_this_category": 100,
                "path_from_root": [{"id": "MLB1051"}, {"id": "MLB3813"}, {"id": "MLB9999"}],
                "children_categories": [],
                "settings": {"shipping_options": ["custom"]},
            },
            "MLB5726": {
                "id": "MLB5726",
                "name": "Eletrodomesticos",
                "total_items_in_this_category": 400,
                "path_from_root": [{"id": "MLB5726", "name": "Eletrodomesticos"}],
                "children_categories": [],
                "settings": {},
            },
        }

        with patch.object(client, "_get") as mock_get:
            mock_get.side_effect = [
                bulk_tree,
                [
                    {"id": "BRAND", "name": "Marca", "tags": {"required": True}, "value_type": "string"},
                    {"id": "GTIN", "name": "GTIN", "tags": {"conditional_required": True}, "value_type": "string"},
                    {"id": "COLOR", "name": "Cor", "tags": {"allow_variations": True}, "values": [{"id": "1"}]},
                ],
            ]
            cats = client.fetch_categories()
            self.assertEqual(len(cats), 2)
            self.assertNotIn("translations", cats[0]["raw_payload"])

            details = client.fetch_leaf_enrichment(cats, [])
            self.assertEqual(len(details), 1)
            payload = details[0]["raw_payload"]
            self.assertEqual(payload["category_id"], "MLB1055")
            self.assertTrue(payload["is_leaf"])
            self.assertEqual(payload["category_depth"], 3)
            self.assertEqual(payload["permalink"], "https://lista.mercadolivre.com.br/smartphones")
            self.assertEqual(payload["attribute_summary"]["conditional_required"], 1)
            self.assertEqual(payload["attribute_summary"]["variation_attributes"], 1)

    def test_fetch_categories_fallback_recursive_bfs_and_ranks_1_to_3(self) -> None:
        cfg = {
            "site_id": "MLB",
            "request_interval_ms": 0,
            "leaf_discovery": {
                "use_bulk_tree": True,
                "max_depth": 4,
                "max_children_per_node": 2,
                "top_leaves_per_macro": 1,
            },
            "target_categories": [{"id": "MLB1051", "name": "Celulares"}],
            "tracked_product_queries": ["Fone Bluetooth"],
        }
        client = self._build_client(is_oauth=False, config_override=cfg)

        req = httpx.Request("GET", "https://api.mercadolibre.com/sites/MLB/categories/all")
        err_403 = httpx.HTTPStatusError("403", request=req, response=httpx.Response(403, request=req))

        responses = {
            "/sites/MLB/categories/all": err_403,
            "/categories/MLB1051": {
                "id": "MLB1051",
                "name": "Celulares",
                "path_from_root": [{"id": "MLB1051"}],
                "children_categories": [{"id": "MLB3813", "total_items_in_this_category": 800}],
                "total_items_in_this_category": 1000,
                "settings": {},
            },
            "/categories/MLB3813": {
                "id": "MLB3813",
                "name": "Acessorios",
                "path_from_root": [{"id": "MLB1051"}, {"id": "MLB3813"}],
                "children_categories": [{"id": "MLB1055", "total_items_in_this_category": 500}],
                "total_items_in_this_category": 800,
                "settings": {},
            },
            "/categories/MLB1055": {
                "id": "MLB1055",
                "name": "Smartphones",
                "path_from_root": [{"id": "MLB1051"}, {"id": "MLB3813"}, {"id": "MLB1055"}],
                "children_categories": [],
                "total_items_in_this_category": 500,
                "settings": {},
            },
            "/sites/MLB/domain_discovery/search": [
                {"category_id": "MLB196208", "category_name": "Fones de Ouvido"},
                {"category_id": "MLB1664", "category_name": "Fones"},
                {"category_id": "MLB74092", "category_name": "Headsets"},
            ],
            "/categories/MLB196208": {"id": "MLB196208", "name": "Fones de Ouvido", "children_categories": [], "path_from_root": [{"id": "MLB1000"}, {"id": "MLB196208"}], "total_items_in_this_category": 100, "settings": {}},
            "/categories/MLB1664": {"id": "MLB1664", "name": "Fones", "children_categories": [], "path_from_root": [{"id": "MLB1000"}, {"id": "MLB1664"}], "total_items_in_this_category": 50, "settings": {}},
            "/categories/MLB74092": {"id": "MLB74092", "name": "Headsets", "children_categories": [], "path_from_root": [{"id": "MLB1000"}, {"id": "MLB74092"}], "total_items_in_this_category": 25, "settings": {}},
        }

        def fake_get(endpoint, params=None):
            if endpoint.endswith("/attributes"):
                return [{"id": "BRAND", "tags": {"required": True}}]
            val = responses[endpoint]
            if isinstance(val, Exception):
                raise val
            return val

        with patch.object(client, "_get", side_effect=fake_get):
            cats = client.fetch_categories()
            trends = client.fetch_trends_and_domains()
            details = client.fetch_leaf_enrichment(cats, trends)

        enriched_ids = [d["category_id"] for d in details]
        self.assertEqual(enriched_ids, ["MLB196208", "MLB1664", "MLB74092", "MLB1055"])

    def test_fetch_payment_methods_deduplicates_ids_and_aggregates_types(self) -> None:
        client = self._build_client(is_oauth=False)
        with patch.object(client, "_get") as mock_get:
            mock_get.side_effect = [
                [
                    {"id": "visa", "payment_type_id": "credit_card"},
                    {"id": "visa", "payment_type_id": "prepaid_card"},
                    {"id": "pix", "payment_type_id": "bank_transfer"},
                ],
                {"id": "visa", "name": "Visa", "payment_type_id": "credit_card", "payer_costs": [{"installments": 12}]},
                {"id": "pix", "name": "Pix", "payment_type_id": "bank_transfer", "payer_costs": [{"installments": 1}]},
            ]
            records = client.fetch_payment_methods()

        self.assertEqual(len(records), 2)
        self.assertEqual(mock_get.call_count, 3)
        visa_payload = records[0]["raw_payload"]
        self.assertEqual(visa_payload["supported_payment_types"], ["credit_card", "prepaid_card"])
        self.assertEqual(visa_payload["currency_reference"], {"id": "BRL"})

    def test_oauth_mode_snapshot_uses_highlights_search_and_multiget(self) -> None:
        cfg = {
            "site_id": "MLB",
            "request_interval_ms": 0,
            "target_categories": [{"id": "MLB1051", "name": "Celulares"}],
            "tracked_product_queries": [],
        }
        client = self._build_client(is_oauth=True, config_override=cfg)
        with (
            patch.object(client, "fetch_highlights", return_value=["MLB1"]),
            patch.object(client, "fetch_search_item_ids", return_value=["MLB1", "MLB2"]),
            patch.object(client, "fetch_items_multiget", return_value=[{"category_id": "MLB1055"}]) as mock_mg,
        ):
            res = client.fetch_items_or_niches_snapshot([], [])

        self.assertEqual(len(res), 1)
        mock_mg.assert_called_once_with(["MLB1", "MLB1", "MLB2"])


if __name__ == "__main__":
    unittest.main()
