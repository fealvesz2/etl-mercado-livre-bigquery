import unittest
from unittest.mock import MagicMock, patch

import httpx

from src.meli_client import MeliClient


class TestMeliClient(unittest.TestCase):
    def _build_client(self, is_oauth: bool) -> MeliClient:
        mock_auth = MagicMock()
        mock_auth.get_auth_headers.return_value = (
            {"Accept": "application/json", "Authorization": "Bearer APP_USR-TEST"}
            if is_oauth
            else {"Accept": "application/json"}
        )
        return MeliClient(config_path="config/categories.yaml", auth_manager=mock_auth)

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

    def test_retry_on_429_and_fail_fast_on_400(self) -> None:
        client = self._build_client(is_oauth=False)
        client._get.retry.sleep = lambda _: None

        req = httpx.Request("GET", "https://api.mercadolibre.com/categories/MLB1051")
        resp_429 = httpx.Response(429, request=req, text="rate limit")
        resp_200 = httpx.Response(200, request=req, json={"id": "MLB1051"})
        resp_400 = httpx.Response(400, request=req, text="bad request")

        with patch("src.meli_client.httpx.Client") as mock_http:
            http_instance = mock_http.return_value.__enter__.return_value
            http_instance.get.side_effect = [resp_429, resp_200]
            data = client._get("/categories/MLB1051")
            self.assertEqual(data["id"], "MLB1051")
            self.assertEqual(http_instance.get.call_count, 2)

        with patch("src.meli_client.httpx.Client") as mock_http:
            http_instance = mock_http.return_value.__enter__.return_value
            http_instance.get.return_value = resp_400
            with self.assertRaises(httpx.HTTPStatusError):
                client._get("/categories/INVALID")
            self.assertEqual(http_instance.get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
