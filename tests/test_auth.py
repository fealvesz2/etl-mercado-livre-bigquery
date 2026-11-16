import os
import unittest
from unittest.mock import MagicMock, patch

from src.auth import MELI_OAUTH_URL, PUBLIC_MODE_SENTINEL, MeliAuthManager


class TestMeliAuthManager(unittest.TestCase):
    def test_init_raises_value_error_when_project_id_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True), patch("src.auth.secretmanager.SecretManagerServiceClient"):
            with self.assertRaises(ValueError):
                MeliAuthManager(project_id=None)

    def test_get_auth_headers_returns_public_headers_for_sentinels(self) -> None:
        for sentinel in (PUBLIC_MODE_SENTINEL, "PENDING_OAUTH"):
            with self.subTest(sentinel=sentinel):
                mock_sm = MagicMock()
                secret_resp = MagicMock()
                secret_resp.payload.data = f"{sentinel}\n".encode("utf-8")
                mock_sm.access_secret_version.return_value = secret_resp

                with patch("src.auth.secretmanager.SecretManagerServiceClient", return_value=mock_sm):
                    manager = MeliAuthManager(project_id="test-project")
                    headers = manager.get_auth_headers()

                self.assertEqual(
                    headers,
                    {"Accept": "application/json", "User-Agent": "MeliDataPlatformETL/1.0"},
                )
                self.assertNotIn("Authorization", headers)
                mock_sm.access_secret_version.assert_called_once_with(
                    request={"name": "projects/test-project/secrets/MELI_APP_ID/versions/latest"}
                )

    def test_get_auth_headers_oauth_refreshes_rotates_and_cleans_up_old_versions(self) -> None:
        mock_sm = MagicMock()

        def access_side_effect(request):
            name = request["name"]
            resp = MagicMock()
            if "MELI_APP_ID" in name:
                resp.payload.data = b"123456"
            elif "MELI_CLIENT_SECRET" in name:
                resp.payload.data = b"sec-xyz"
            elif "MELI_REFRESH_TOKEN" in name:
                resp.payload.data = b"TG-OLD"
            return resp

        mock_sm.access_secret_version.side_effect = access_side_effect
        new_ver = MagicMock()
        new_ver.name = "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/10"
        mock_sm.add_secret_version.return_value = new_ver

        v10, v3, v2 = MagicMock(), MagicMock(), MagicMock()
        v10.name = "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/10"
        v3.name = "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/3"
        v2.name = "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/2"
        mock_sm.list_secret_versions.return_value = [v2, v10, v3]

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "access_token": "APP_USR-NEW",
            "refresh_token": "TG-NEW",
            "expires_in": 21600,
        }

        with (
            patch("src.auth.secretmanager.SecretManagerServiceClient", return_value=mock_sm),
            patch("src.auth.httpx.Client") as mock_http,
        ):
            http_instance = mock_http.return_value.__enter__.return_value
            http_instance.post.return_value = mock_resp

            manager = MeliAuthManager(project_id="test-project")
            headers = manager.get_auth_headers()

        self.assertEqual(headers["Authorization"], "Bearer APP_USR-NEW")
        http_instance.post.assert_called_once_with(
            MELI_OAUTH_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": "123456",
                "client_secret": "sec-xyz",
                "refresh_token": "TG-OLD",
            },
            headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        )
        mock_sm.add_secret_version.assert_called_once_with(
            request={
                "parent": "projects/test-project/secrets/MELI_REFRESH_TOKEN",
                "payload": {"data": b"TG-NEW"},
            }
        )
        destroyed = [call.kwargs["request"]["name"] for call in mock_sm.destroy_secret_version.call_args_list]
        self.assertEqual(
            destroyed,
            [
                "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/3",
                "projects/test-project/secrets/MELI_REFRESH_TOKEN/versions/2",
            ],
        )

    def test_refresh_oauth_token_skips_rotation_when_refresh_token_unchanged(self) -> None:
        mock_sm = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "access_token": "APP_USR-SAME",
            "refresh_token": "TG-SAME",
            "expires_in": 21600,
        }

        with (
            patch("src.auth.secretmanager.SecretManagerServiceClient", return_value=mock_sm),
            patch("src.auth.httpx.Client") as mock_http,
        ):
            mock_http.return_value.__enter__.return_value.post.return_value = mock_resp
            manager = MeliAuthManager(project_id="test-project")
            token = manager.refresh_oauth_token("123", "sec", "TG-SAME")

        self.assertEqual(token, "APP_USR-SAME")
        mock_sm.add_secret_version.assert_not_called()


if __name__ == "__main__":
    unittest.main()
