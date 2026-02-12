from dataclasses import dataclass

from src.pipeline.core.logging import get_logger
from src.pipeline.core.config import BRONZE_DIR, BREWERIES_DATASET

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

logger = get_logger(__name__)


@dataclass(frozen=True)
class BronzeBreweryReader:
    
    """
    Lê os dados da camada Bronze de cervejarias.
    """
    
    bronze_base_path: str = "data/bronze"
    dataset_name: str = "breweries"
    schema: StructType | None = None

    def path(self, ingestion_date: str) -> str:

        return str(
            BRONZE_DIR
            / BREWERIES_DATASET
            / f"ingestion_date={ingestion_date}"
            / f"{BREWERIES_DATASET}.json"
        )

    def read(self, spark: SparkSession, ingestion_date: str) -> DataFrame:

        p = self.path(ingestion_date)

        logger.info(
            "Reading Bronze data | ingestion_date=%s | path=%s",
            ingestion_date,
            p,
        )
    
        if self.schema is not None:
            return spark.read.option("multiline", "true").schema(self.schema).json(p)
        
        return spark.read.option("multiline", "true").json(p)
