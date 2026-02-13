from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.core.config import SILVER_DIR, BREWERIES_DATASET
from pipeline.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class SilverBreweriesReader:
    """
    Lê dados da camada Silver para alimentar a camada Gold.
    """
    
    silver_dir: str = str(SILVER_DIR / BREWERIES_DATASET)

    def read(
        self, 
        spark: SparkSession, 
        execution_date: str
    ) -> DataFrame:

        """
        Lê os dados de cervejarias da camada Silver para a data de execução especificada.

        Args:
            spark (SparkSession): A sessão Spark ativa.
            execution_date (str): A data de execução no formato 'yyyy-MM-dd'.

        Returns:
            DataFrame: Um DataFrame contendo os dados das cervejarias.
        """
        logger.info(
            "Reading Silver data | ingestion_date=%s | path=%s",
            execution_date,
            self.silver_dir,
        )

        silver_df = (
            spark.read.parquet(self.silver_dir)
            .filter(F.col("ingestion_date") == execution_date)
        )

        logger.info("Silver read finished successfully")
        return silver_df