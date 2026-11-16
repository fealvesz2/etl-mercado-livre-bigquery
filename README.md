# Pipeline ETL Serverless: Monitoramento Mercado Livre (MLB) no GCP

Pipeline de Engenharia de Dados ponta a ponta construido em **Python 3.12**, **Google BigQuery** (Arquitetura Medalhao: **Bronze / Silver / Gold**), **Secret Manager**, **Artifact Registry**, **Cloud Run Jobs** e **Cloud Scheduler**.

Projetado com foco em **FinOps**, **idempotencia diaria**, **principio do menor privilegio (IAM)** e **resiliencia de API** para execucao serverless em **`southamerica-east1`**.

---

## 1. Arquitetura Geral

```mermaid
flowchart LR
  SCHED["Cloud Scheduler (07h BRT, retry finops)"] -->|"POST :run (OAuth)"| JOB["Cloud Run Job v2 (Non-Root, 1 vCPU, 512Mi)"]
  SM["Secret Manager (MELI_*)"] -->|"Dual-Mode Auth"| JOB
  MELI["API Mercado Livre (api.mercadolibre.com)"] -->|"HTTPS Pacing + Retry-After"| JOB
  JOB -->|"Batch Load $0 ($YYYYMMDD)"| BZ["BigQuery: meli_bronze"]
  BZ -->|"SQL Parametrizado + QUALIFY"| SV["BigQuery: meli_silver"]
  SV -->|"Views Janela 30d"| GD["BigQuery: meli_gold"]
```

### Destaques de Engenharia

* **Conector Dual-Mode (`src/auth.py` e `src/meli_client.py`):**
  * **`OAUTH_MODE`:** Renovacao automatica via `POST /oauth/token`, rotacao atomica do `refresh_token` *single-use* no Secret Manager (destruindo versoes antigas), busca de `/highlights` + `/search` e enriquecimento via Multiget em lotes de ate `20` IDs (`/items?ids=...`).
  * **`PUBLIC_MODE` (Extracao publica maximizada):** Quando credenciais OAuth ainda nao estao configuradas (`MELI_APP_ID=PUBLIC_MODE`), utiliza sessao HTTP persistente com pacing configuravel (`request_interval_ms=80`) e tratamento de `Retry-After` para coletar:
    1. **Taxonomia MLB completa (`GET /sites/MLB/categories/all`):** captura todas as `32` macro-categorias raiz (removendo bloco `translations`) e seleciona recursivamente as top `10` folhas reais (`is_leaf=true`, profundidade ate `7`) de cada uma das `15` macros comerciais configuradas, com fallback automatico para BFS recursivo em `/categories/{id}`.
    2. **Descoberta semantica (`GET /sites/MLB/domain_discovery/search`):** `40` queries monitoradas com enriquecimento completo dos ranks `1`, `2` e `3` e ficha tecnica completa em `/categories/{id}/attributes` (`required`, `catalog_required`, `conditional_required`, `allow_variations`, `variation_attribute`, `GTIN`).
    3. **Catalogo de meios de pagamento (`GET /sites/MLB/payment_methods` e `/{id}`):** deduplicacao por identificador, consolidacao de tipos aceitos (`credit_card`, `prepaid_card`, `bank_transfer`, `ticket`, `digital_currency`) e faixas de parcelamento (`payer_costs`).
* **Ingestao Bronze Idempotente a Custo Zero (`src/bronze_loader.py`):**
  * Carrega 4 tabelas (`raw_categories`, `raw_trends`, `raw_items_search`, `raw_payment_methods`) via **BigQuery Batch Load Jobs** (`load_table_from_json` em NDJSON) com *partition decorator* `$YYYYMMDD` e `WRITE_TRUNCATE`.
  * Schema explicito (`BRONZE_SCHEMA`) com `autodetect=False`, preservando `raw_payload` como tipo nativo `JSON NOT NULL`.
* **Camada Silver Transacional & Deduplicada (`src/silver_transformer.py`):**
  * Executa 4 scripts SQL parametrizados (`@execution_date`) em transacao atomica (`BEGIN TRANSACTION; DELETE ...; INSERT ...; COMMIT TRANSACTION;`), com `QUALIFY ROW_NUMBER() OVER (...) = 1`.
  * `dim_categories`: hierarquia macro + folhas reais com `category_depth`, `vertical`, `sub_vertical`, `shipping_profile`, `maximum_price`, `max_pictures_per_item`, `max_variations_allowed`, `conditional_required_attributes_count`, `variation_attributes_count` e `permalink`.
  * `fact_daily_item_prices`: regra deterministica de `logistic_type` (priorizando `carrier` sobre `custom` independente da ordem do array `settings.shipping_options` da API MLB).
  * `fact_daily_trends`: expansao completa dos ranks `1`, `2` e `3` de `domain_discovery` e `official_trends`.
  * `dim_payment_methods`: prazos de liquidacao (`min_accreditation_days`, `max_accreditation_days`), `max_installments`, `max_installments_no_interest` e `ARRAY<STRUCT>` `payer_costs`.
* **Camada Gold Pronta para BI (`sql/03_gold_views.sql`):**
  * `vw_price_monitoring` (+ `vw_price_monitoring_latest`): precos piso/teto, descontos, ofertas ativas, adocao de catalogo e exigencia de GTIN/atributos por categoria/nicho.
  * `vw_logistics_share` (+ `vw_logistics_share_latest`): distribuicao deterministica por modalidade logistica (`fulfillment`, `carrier`, `custom`) e cobertura de frete gratis.
  * `vw_trends_vs_offers` (+ `vw_trends_vs_offers_latest`): cruzamento entre termos monitorados (ranks 1 a 3), marca/modelo inferidos, concorrencia ativa e indice de oportunidade (sem registros `NAO_ENRIQUECIDA` no snapshot mais recente).
  * `vw_payment_installments` (+ `vw_payment_installments_latest`): comparativo de meios de pagamento, liquidacao e faixas de parcelamento no MLB.
  * As views `*_latest` isolam o snapshot mais recente para consumo direto em paineis BI sem apagar o historico da janela de 30 dias.

---

## 2. Governanca, Seguranca & FinOps

| Pilar | Controle Implementado |
| :--- | :--- |
| **Particionamento & Custos** | `PARTITION BY execution_date` / `snapshot_date`, `CLUSTER BY`, `require_partition_filter = TRUE` em Bronze/Silver, expiracao automatica de `90` dias na Bronze e views Gold com janela deslizante de `30` dias. |
| **Trava de Leitura SQL** | `maximum_bytes_billed = 52428800` (`50 MiB`) em cada transformacao Silver (abortando queries fora do orcamento antes da cobranca). |
| **Cloud Scheduler FinOps** | `etl-meli-daily` (`0 7 * * *`, `America/Sao_Paulo`) com `attemptDeadline = 360s` e `retryConfig` limitado (`retryCount = 2`, `minBackoff = 60s`, `maxBackoff = 300s`, `maxRetryDuration = 600s`, `maxDoublings = 2`). |
| **Container Hardening** | `Dockerfile` multi-stage (`python:3.12-slim` + `uv 0.12.23`) executando sob usuario non-root (`USER 10001:10001`); `.dockerignore` bloqueia `.env`, `.git/`, `.venv/` e `*.json`. |
| **IAM Menor Privilegio** | Service Account dedicada (`sa-etl-meli`) com `roles/bigquery.jobUser`, permissao `WRITER` restrita aos datasets `meli_bronze`/`meli_silver`, acesso granular por segredo no Secret Manager e `roles/run.invoker` apenas no job alvo. |
| **Artifact Registry** | Politica de retencao automatica mantendo apenas as `2` versoes mais recentes (`keepCount: 2`) e removendo digests *untagged* apos `24h`. |

### Baseline Validado em Nuvem (`v2` em `southamerica-east1`, execucao `etl-meli-job-r5z22`)

* **Tempo de execucao no Cloud Run Job (`1` vCPU, `512Mi`, timeout `300s`):** `1m36.86s` (`96.86s`), sem erros `429`.
* **Ingestao Bronze diaria:** `286` registros (`raw_categories=32`, `raw_trends=40`, `raw_items_search=206`, `raw_payment_methods=8`).
* **Consolidacao Silver diaria:** `524` registros (`dim_categories=238`, `fact_daily_item_prices=206`, `fact_daily_trends=72`, `dim_payment_methods=8`), consumindo `90.0 MiB` totais em 4 transacoes (maximo `30 MiB` por query).
* **Qualidade Gold (`vw_trends_vs_offers`):** `72` linhas diarias nos ranks `1` a `3` com `0` ocorrencias `NAO_ENRIQUECIDA`.

---

## 3. Estrutura do Repositorio

```text
.
|-- config/
|   `-- categories.yaml          # 15 macros, parametros de folhas reais e 40 queries MLB
|-- sql/
|   |-- 01_bronze_ddl.sql        # DDL particionada/clusterizada (meli_bronze)
|   |-- 02_silver_ddl.sql        # DDL tipada dual-mode e evolucao de colunas (meli_silver)
|   |-- 03_gold_views.sql        # 8 Views analiticas Gold (janela 30d + *_latest)
|   `-- transforms/              # Scripts SQL parametrizados Bronze -> Silver
|-- src/
|   |-- auth.py                  # Dual-Mode Auth + rotacao no Secret Manager
|   |-- meli_client.py           # Cliente HTTP persistente (Pacing, Retry-After, Bulk/BFS)
|   |-- bronze_loader.py         # Batch Loader $0 com overwrite por particao
|   |-- silver_transformer.py    # Executor SQL transacional com trava FinOps
|   `-- main.py                  # Orquestrador CLI (--execution-date, --skip-extract)
|-- tests/                       # 26 testes unitarios offline (unittest + mocks)
|-- .dockerignore
|-- .env.example
|-- Dockerfile
|-- GEMINI.md
`-- pyproject.toml
```

---

## 4. Setup Local, Testes e Execucao

### Pre-requisitos
* Python 3.12+ e `uv`
* Google Cloud SDK (`gcloud` e `bq`) autenticado via ADC (`gcloud auth application-default login`)

### Instalacao e Testes Unitarios

```powershell
cp .env.example .env
# Preencha GCP_PROJECT_ID no arquivo .env (nao versionado)

uv sync
uv run python -m unittest discover -s tests -v
```

### Aplicar DDLs e Views no BigQuery

```powershell
$PROJECT_ID = "seu-projeto-gcp"

Get-Content "sql\01_bronze_ddl.sql" -Raw | bq query --project_id=$PROJECT_ID --location=southamerica-east1 --use_legacy_sql=false
Get-Content "sql\02_silver_ddl.sql" -Raw | bq query --project_id=$PROJECT_ID --location=southamerica-east1 --use_legacy_sql=false
Get-Content "sql\03_gold_views.sql" -Raw | bq query --project_id=$PROJECT_ID --location=southamerica-east1 --use_legacy_sql=false
```

### Executar o Pipeline Localmente

```powershell
# Execucao completa (Extract -> Bronze -> Silver) para a data UTC atual
uv run python -m src.main

# Reprocessar apenas Silver de uma data especifica (backfill idempotente)
uv run python -m src.main --execution-date 2026-11-16 --skip-extract
```

---

## 5. Deploy Serverless no GCP (`southamerica-east1`)

```powershell
$PROJECT_ID = "seu-projeto-gcp"
$REGION     = "southamerica-east1"
$REPO       = "etl-meli"
$IMAGE      = "$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/etl-meli-job:v2"
$SA_EMAIL   = "sa-etl-meli@$PROJECT_ID.iam.gserviceaccount.com"

# 1. Build remoto via Cloud Build em southamerica-east1
gcloud builds submit --project=$PROJECT_ID --region=$REGION --tag=$IMAGE .

# 2. Deploy do Cloud Run Job (colocalizado com o BigQuery em southamerica-east1)
gcloud run jobs deploy etl-meli-job `
  --project=$PROJECT_ID `
  --region=$REGION `
  --image=$IMAGE `
  --service-account=$SA_EMAIL `
  --cpu=1 `
  --memory=512Mi `
  --max-retries=1 `
  --task-timeout=300s `
  --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,BQ_LOCATION=$REGION,BQ_DATASET_BRONZE=meli_bronze,BQ_DATASET_SILVER=meli_silver,BQ_MAX_BYTES_BILLED=52428800"

# 3. Ajuste FinOps de retry no Cloud Scheduler
gcloud scheduler jobs update http etl-meli-daily `
  --project=$PROJECT_ID `
  --location=$REGION `
  --max-retry-attempts=2 `
  --min-backoff=60s `
  --max-backoff=300s `
  --max-retry-duration=600s `
  --max-doublings=2 `
  --attempt-deadline=360s

# 4. Disparo manual validado
gcloud run jobs execute etl-meli-job --project=$PROJECT_ID --region=$REGION --wait
```
