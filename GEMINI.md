# GEMINI.md · gestor-anuncios-mercado-livre

> Regras locais do pipeline Serverless ETL (Mercado Livre MLB -> BigQuery -> Cloud Run Jobs).

## Ambiente GCP deste Repositorio
- **Projeto GCP:** `dev-plataforma-geral` (nao confundir com `prd-plataforma-geral`).
- **Regiao:** `southamerica-east1`.
- **Datasets:** `meli_bronze`, `meli_silver`, `meli_gold`.
- **Service Account de execucao:** `sa-etl-meli@dev-plataforma-geral.iam.gserviceaccount.com` (principio do menor privilegio).

## Guardrails Tecnicos
- Autenticacao Dual-Mode (`src/auth.py`): respeitar o sentinela `PUBLIC_MODE` no Secret Manager quando credenciais OAuth ainda nao estiverem provisionadas.
- Queries e transformacoes no BigQuery devem usar `maximum_bytes_billed` compativel com o minimo de 10 MiB por tabela referenciada (ex.: `52428800` bytes / 50 MiB em scripts DML).
- Rodar testes unitarios com `uv run python -m unittest discover -s tests -v` antes de encerrar alteracoes em `src/` ou `sql/`.
- Nao executar `git commit` ou `git push` automaticamente; entregar mudancas verificadas para commit manual do usuario.
- Nunca utilizar travessao longo na documentacao do projeto.
