from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.pipeline.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class BreweryGoldTransformer:

    """
    Recebe os dados da camada Silver e gera um ranking dos estados com mais cervejarias por país e dia.
    Regras de transformação:
    - Agrega por estado, país e dia, contando o número de cervejarias, cidades e tipos de cervejarias
    - Calcula o total de cervejarias por país e dia para calcular a porcentagem do total do país
    - Gera um ranking dos estados por país e dia, mantendo apenas os TOP N (configurável) estados
    """

    TOP_N: int = 5

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info("Starting Gold transformation (states ranking)")

        # Agrega por estado (por dia e país)
        by_state = (
            df
            .groupBy("ingestion_date", "country", "state")
            .agg(
                F.countDistinct("id").alias("brewery_count"),
                F.countDistinct("city").alias("city_count"),
                F.countDistinct("brewery_type").alias("types_count")
            )
        )

        # Total por país/dia para % do país
        country_total = (
            by_state
            .groupBy("ingestion_date", "country")
            .agg(
                F.sum("brewery_count").alias("country_brewery_total")
            )
        )

        enriched = (
            by_state
            .join(country_total, ["ingestion_date", "country"], "left")
            .withColumn("pct_country_total",
                F.when(F.col("country_brewery_total") > 0,
                       F.col("brewery_count") / F.col("country_brewery_total"))
                .otherwise(F.lit(0.0))
            )
        )

        w = Window.partitionBy("ingestion_date", "country").orderBy(F.col("brewery_count").desc())

        ranked = (
            enriched
            .withColumn("rank_state_in_country", F.row_number().over(w))
            .filter(F.col("rank_state_in_country") <= F.lit(self.TOP_N))
            .withColumn("created_at_utc", F.current_timestamp())
        )

        logger.info("Gold transformation finished successfully")
        return ranked
