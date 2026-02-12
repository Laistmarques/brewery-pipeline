from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.pipeline.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class BreweryGoldTransformer:

    """
    Silver -> Gold: agrega os dados para consumo
        - Agrupa por estado e calcula a média de ABV, IBU e EBC
        - Conta o número de cervejarias por estado
    """

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Silver -> Gold: agrega dados para consumo (métricas por localização e tipo).

        Args:
            df (DataFrame): O DataFrame de entrada com os dados das cervejarias.

        Returns:
            DataFrame: Um DataFrame transformado com as métricas agregadas por estado.
        """
        logger.info("Starting transformation of Silver data to Gold format")

        logger.debug("Input columns: %s", df.columns)

        gold_df = (
            df.groupBy("ingestion_date", "country", "state", "brewery_type")
              .agg(
                  F.countDistinct("id").alias("brewery_count"),
                  F.countDistinct("city").alias("city_count"),
              )
              .withColumn("created_at_utc", F.current_timestamp())
        )


        logger.info("Transformation to Gold format finished successfully")
        return gold_df