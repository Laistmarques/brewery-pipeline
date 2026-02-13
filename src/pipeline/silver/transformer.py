from dataclasses import dataclass
from pipeline.core.logging import get_logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = get_logger(__name__)


@dataclass(frozen=True)
class BrewerySilverTransformer:
    
    """
    Regras de transformação para os dados de cervejarias na camada Silver.
        - Seleciona as colunas relevantes
        - Renomeia colunas para padronização
        - Adiciona a data de ingestão como coluna
    """

    def transform(self, df: DataFrame, ingestion_date: str) -> DataFrame:

        logger.info("Starting Silver transform | ingestion_date=%s", ingestion_date)

        logger.debug("Input columns: %s", df.columns)
        
        selected_columns = [
            "id",
            "name",
            "brewery_type",
            "city",
            "state_province",
            "country",
            "longitude",
            "latitude",
        ]

        missing = [c for c in selected_columns if c not in df.columns]

        if missing:
            raise ValueError(f"Missing required columns for Silver: {missing}")

        df2 = (
            df
            .select(*selected_columns)
            .withColumn("state", F.col("state_province"))
            .drop("state_province")
            .withColumn("ingestion_date", F.lit(ingestion_date))
        )

        df2 = df2.filter(F.col("country").isNotNull() & F.col("state").isNotNull())

        logger.info("Silver transform finished successfully")

        return df2