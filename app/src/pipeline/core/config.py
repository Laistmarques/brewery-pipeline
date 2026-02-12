from pathlib import Path

# ========= BASE =========
BASE_DIR = Path("/app/data")

# ========= CAMADAS =========
BRONZE_DIR = BASE_DIR / "bronze"
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR   = BASE_DIR / "gold"

# ========= DATASETS =========
BREWERIES_DATASET = "breweries"
GOLD_BREWERIES_BY_LOCATION = "breweries_by_location"

# ========= FORMATOS =========
BRONZE_FORMAT = "json"
SILVER_FORMAT = "parquet"
GOLD_FORMAT   = "parquet"

# ========= PARTIÇÕES =========
SILVER_PARTITIONS = ["ingestion_date", "country"]
GOLD_PARTITIONS   = ["ingestion_date", "country"]
BRONZE_PARTITION_KEY = "ingestion_date"

# ========= EXECUÇÃO =========
DEFAULT_APP_NAME = "BreweryPipeline"
DEFAULT_WRITE_MODE = "overwrite"
