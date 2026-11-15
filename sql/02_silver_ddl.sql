CREATE TABLE IF NOT EXISTS meli_silver.dim_categories (
  snapshot_date DATE NOT NULL OPTIONS(description="Data logica do snapshot diario (chave de particao)"),
  category_id STRING NOT NULL OPTIONS(description="ID da categoria MLB (macro ou folha/nicho)"),
  category_name STRING NOT NULL OPTIONS(description="Nome oficial da categoria no Mercado Livre"),
  parent_category_id STRING OPTIONS(description="ID da macro-categoria raiz em path_from_root[0]"),
  parent_category_name STRING OPTIONS(description="Nome da macro-categoria raiz em path_from_root[0]"),
  category_level STRING NOT NULL OPTIONS(description="Nivel hierarquico: MACRO ou NICHE_OR_LEAF"),
  is_leaf BOOL NOT NULL OPTIONS(description="Indica se e categoria folha sem subcategorias"),
  listing_allowed BOOL OPTIONS(description="Indica se a categoria permite publicacao direta"),
  catalog_domain STRING OPTIONS(description="Dominio de catalogo MLB associado (ex: MLB-CELLPHONES)"),
  total_items INT64 NOT NULL OPTIONS(description="Volume total de anuncios ativos na categoria"),
  children_count INT64 NOT NULL OPTIONS(description="Quantidade de subcategorias filhas diretas"),
  total_attributes_count INT64 OPTIONS(description="Total de atributos tecnicos da categoria"),
  required_attributes_count INT64 OPTIONS(description="Qtd de atributos obrigatorios para publicacao"),
  catalog_required_attributes_count INT64 OPTIONS(description="Qtd de atributos obrigatorios para Catalogo/Buy Box"),
  shipping_options ARRAY<STRING> OPTIONS(description="Modalidades de envio aceitas (ex: carrier, custom)"),
  buying_modes ARRAY<STRING> OPTIONS(description="Modalidades de compra aceitas (ex: buy_it_now)"),
  minimum_price NUMERIC OPTIONS(description="Preco minimo permitido para anuncios na categoria"),
  permalink STRING OPTIONS(description="URL publica oficial da categoria no Mercado Livre"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC da coleta original na Bronze"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 do payload original para rastreabilidade")
)
PARTITION BY snapshot_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Silver - Dimensao diaria de macro-categorias e nichos folha MLB",
  require_partition_filter=TRUE,
  labels=[("domain", "meli"), ("layer", "silver")]
);

CREATE TABLE IF NOT EXISTS meli_silver.fact_daily_item_prices (
  snapshot_date DATE NOT NULL OPTIONS(description="Data logica do snapshot diario (chave de particao)"),
  record_id STRING NOT NULL OPTIONS(description="Chave unica do registro (item_id em OAuth ou category_id em Public)"),
  category_id STRING NOT NULL OPTIONS(description="ID da categoria MLB associada"),
  category_name STRING OPTIONS(description="Nome da categoria MLB associada"),
  data_mode STRING NOT NULL OPTIONS(description="Modo de origem: OAUTH_MULTIGET ou PUBLIC_NICHE_SPEC"),
  title STRING OPTIONS(description="Titulo do anuncio (OAuth) ou nome do nicho (Public)"),
  user_product_id STRING OPTIONS(description="ID do User Product MLB quando aplicavel"),
  price NUMERIC OPTIONS(description="Preco vigente do item (OAuth) ou preco piso da categoria (Public)"),
  base_price NUMERIC OPTIONS(description="Preco base sem promocoes (OAuth)"),
  original_price NUMERIC OPTIONS(description="Preco original riscado antes de desconto (OAuth)"),
  discount_pct NUMERIC OPTIONS(description="Percentual de desconto calculado sobre original_price"),
  currency_id STRING OPTIONS(description="Moeda do anuncio (padrao BRL)"),
  available_quantity INT64 OPTIONS(description="Estoque disponivel (OAuth) ou total de ofertas no nicho (Public)"),
  sold_quantity INT64 OPTIONS(description="Unidades vendidas reportadas (OAuth)"),
  condition STRING OPTIONS(description="Condicao do produto (new, used, etc.)"),
  status STRING OPTIONS(description="Status do anuncio ou status da categoria"),
  catalog_listing BOOL OPTIONS(description="Indica se o anuncio participa do Catalogo/Buy Box"),
  logistic_type STRING OPTIONS(description="Tipo logistico (fulfillment, xd_drop_off, carrier, etc.)"),
  free_shipping BOOL OPTIONS(description="Indica se o anuncio oferece frete gratis"),
  supports_carrier_shipping BOOL OPTIONS(description="Flag logistica de suporte a Mercado Envios / Full"),
  requires_gtin BOOL OPTIONS(description="Flag se a categoria folha exige codigo GTIN/EAN"),
  required_attrs_count INT64 OPTIONS(description="Quantidade de atributos obrigatorios no nicho"),
  catalog_required_attrs_count INT64 OPTIONS(description="Quantidade de atributos exigidos para Catalogo"),
  permalink STRING OPTIONS(description="Link oficial do anuncio ou categoria"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC da coleta original na Bronze"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 do payload original para rastreabilidade")
)
PARTITION BY snapshot_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Silver - Fato diario dual-mode de precos, descontos, logistica e nichos MLB",
  require_partition_filter=TRUE,
  labels=[("domain", "meli"), ("layer", "silver")]
);

CREATE TABLE IF NOT EXISTS meli_silver.fact_daily_trends (
  snapshot_date DATE NOT NULL OPTIONS(description="Data logica do snapshot diario (chave de particao)"),
  trend_source STRING NOT NULL OPTIONS(description="Origem da tendencia: domain_discovery ou official_trends"),
  search_term STRING NOT NULL OPTIONS(description="Termo de produto monitorado ou keyword oficial de tendencia"),
  rank_position INT64 NOT NULL OPTIONS(description="Posicao de relevancia do match ou ranking na categoria"),
  category_id STRING NOT NULL OPTIONS(description="ID da categoria MLB associada"),
  category_name STRING OPTIONS(description="Nome da categoria MLB associada"),
  domain_id STRING OPTIONS(description="ID do dominio semantico MLB (ex: MLB-CELLPHONES)"),
  domain_name STRING OPTIONS(description="Nome do dominio semantico MLB"),
  inferred_brand STRING OPTIONS(description="Marca inferida pelo motor semantico do Mercado Livre"),
  inferred_model STRING OPTIONS(description="Modelo inferido pelo motor semantico do Mercado Livre"),
  trend_url STRING OPTIONS(description="URL oficial da tendencia quando disponivel (OAuth)"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp UTC da coleta original na Bronze"),
  payload_hash STRING NOT NULL OPTIONS(description="Hash SHA-256 do payload original para rastreabilidade")
)
PARTITION BY snapshot_date
CLUSTER BY category_id
OPTIONS(
  description="Camada Silver - Fato diario de tendencias de busca e descoberta semantica de dominios MLB",
  require_partition_filter=TRUE,
  labels=[("domain", "meli"), ("layer", "silver")]
);
