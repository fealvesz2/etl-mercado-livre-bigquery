import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
