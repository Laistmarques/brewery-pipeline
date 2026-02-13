from dataclasses import dataclass
from pyspark.sql import SparkSession

from pipeline.core.logging import get_logger
from pipeline.gold.reader import SilverBreweriesReader
from pipeline.gold.transformer import BreweryGoldTransformer
from pipeline.gold.writer import GoldBreweriesWriter

logger = get_logger(__name__)


@dataclass(frozen=True)
class GoldBreweryJob:
    """
    Job principal para a camada Gold, orquestrando leitura, transformação e escrita dos dados.
    """

    reader: SilverBreweriesReader
    transformer: BreweryGoldTransformer
    writer: GoldBreweriesWriter

    def run(self, spark: SparkSession, execution_date: str) -> str:
        """
        Executa o job da camada Gold.

        Args:
            spark (SparkSession): A sessão Spark ativa.
            execution_date (str): A data de execução no formato 'yyyy-MM-dd'.

        Returns:
            str: O caminho onde os dados Gold foram escritos.
        """
        logger.info("Starting Gold job | execution_date=%s", execution_date)

        silver_df = self.reader.read(spark, execution_date)
        logger.info("Silver rows read (filtered by date): %d", silver_df.count())

        gold_df = self.transformer.transform(silver_df)
        logger.info("Gold rows after aggregation: %d", gold_df.count())

        out = self.writer.write(gold_df)

        logger.info("Gold job finished successfully | output_path=%s", out)
        return out
