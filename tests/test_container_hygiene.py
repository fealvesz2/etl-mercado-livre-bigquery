import unittest
from pathlib import Path

import yaml


class TestContainerHygiene(unittest.TestCase):
    def test_dockerfile_uses_slim_python312_and_non_root_user(self) -> None:
        dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM python:3.12-slim AS builder", dockerfile)
        self.assertIn("FROM python:3.12-slim AS runtime", dockerfile)
        self.assertIn("uv sync --frozen --no-dev --no-install-project", dockerfile)
        self.assertIn("USER 10001:10001", dockerfile)
        self.assertIn('ENTRYPOINT ["python", "-m", "src.main"]', dockerfile)
        self.assertNotIn("dev-plataforma-geral", dockerfile)

    def test_dockerignore_blocks_env_secrets_and_venv(self) -> None:
        dockerignore = Path(".dockerignore").read_text(encoding="utf-8").splitlines()
        self.assertIn(".env", dockerignore)
        self.assertIn(".git/", dockerignore)
        self.assertIn(".venv/", dockerignore)
        self.assertIn("*.json", dockerignore)

    def test_categories_yaml_contract_is_valid(self) -> None:
        cfg = yaml.safe_load(Path("config/categories.yaml").read_text(encoding="utf-8"))
        self.assertEqual(cfg["site_id"], "MLB")
        self.assertGreaterEqual(len(cfg["target_categories"]), 15)
        self.assertGreaterEqual(len(cfg["tracked_product_queries"]), 40)
        self.assertIn("leaf_discovery", cfg)
        self.assertGreaterEqual(cfg["leaf_discovery"]["max_depth"], 5)
        self.assertGreaterEqual(cfg["leaf_discovery"]["top_leaves_per_macro"], 5)


if __name__ == "__main__":
    unittest.main()
