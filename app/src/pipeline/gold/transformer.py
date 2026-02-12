from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.pipeline.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class BreweryGoldTransformer:

    """
    Gold Layer (conforme case):
    - Cria uma visão agregada com a quantidade de cervejarias por tipo e localização.

    Entrada esperada (Silver):
    - ingestion_date, country, state, brewery_type, id (e possivelmente outras colunas)

    Saída (Gold):
    - ingestion_date
    - country
    - state
    - brewery_type
    - brewery_count (countDistinct(id))
    - created_at_utc
    """

    def transform(self, df: DataFrame) -> DataFrame:
        
        logger.info("Starting Gold transformation (breweries by type and location)")

        return (
            df
            .groupBy(
                F.col("ingestion_date"),
                F.col("brewery_type"),
                F.col("country"),
                F.col("state"),
                F.col("city"),
            )
            .agg(
                F.countDistinct("id").alias("num_breweries"),
                F.countDistinct("city").alias("num_city"),
            )
            .orderBy(F.desc("num_breweries"))
            .withColumn("created_at_utc", F.current_timestamp())
        )
