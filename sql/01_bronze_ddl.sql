CREATE TABLE IF NOT EXISTS meli_bronze.raw_categories (
  execution_date DATE NOT NULL OPTIONS(description="Data logica da execucao do pipeline (chave de particao)"),
  endpoint STRING NOT NULL OPTIONS(description="Rota da API do Mercado Livre consultada"),
  category_id STRING OPTIONS(description="ID da categoria MLB relacionada ao registro"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC exato da coleta HTTP"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 deterministico do payload JSON"),
  raw_payload JSON NOT NULL OPTIONS(description="Payload bruto retornado pela API do Mercado Livre")
)
PARTITION BY execution_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Bronze - Arvores e metadados das macro-categorias MLB",
  require_partition_filter=TRUE,
  partition_expiration_days=90,
  labels=[("domain", "meli"), ("layer", "bronze")]
);

CREATE TABLE IF NOT EXISTS meli_bronze.raw_trends (
  execution_date DATE NOT NULL OPTIONS(description="Data logica da execucao do pipeline (chave de particao)"),
  endpoint STRING NOT NULL OPTIONS(description="Rota da API do Mercado Livre consultada"),
  category_id STRING OPTIONS(description="ID da categoria MLB relacionada ao registro"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC exato da coleta HTTP"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 deterministico do payload JSON"),
  raw_payload JSON NOT NULL OPTIONS(description="Payload bruto retornado pela API do Mercado Livre")
)
PARTITION BY execution_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Bronze - Tendencias de busca e descoberta de dominios MLB",
  require_partition_filter=TRUE,
  partition_expiration_days=90,
  labels=[("domain", "meli"), ("layer", "bronze")]
);

CREATE TABLE IF NOT EXISTS meli_bronze.raw_items_search (
  execution_date DATE NOT NULL OPTIONS(description="Data logica da execucao do pipeline (chave de particao)"),
  endpoint STRING NOT NULL OPTIONS(description="Rota da API do Mercado Livre consultada"),
  category_id STRING OPTIONS(description="ID da categoria MLB relacionada ao registro"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC exato da coleta HTTP"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 deterministico do payload JSON"),
  raw_payload JSON NOT NULL OPTIONS(description="Payload bruto de itens Multiget (OAuth) ou nichos/atributos (Public)")
)
PARTITION BY execution_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Bronze - Snapshot diario de itens Multiget e enriquecimento de nichos",
  require_partition_filter=TRUE,
  partition_expiration_days=90,
  labels=[("domain", "meli"), ("layer", "bronze")]
);