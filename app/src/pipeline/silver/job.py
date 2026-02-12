from dataclasses import dataclass
from pyspark.sql import SparkSession

from src.pipeline.silver.reader import BronzeBreweryReader
from src.pipeline.silver.transformer import BrewerySilverTransformer
from src.pipeline.silver.writer import SilverBreweryWriter
from src.pipeline.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SilverBreweryJob:

    reader: BronzeBreweryReader
    transformer: BrewerySilverTransformer
    writer: SilverBreweryWriter

    def run(self, spark: SparkSession, execution_date: str) -> str:
        
        logger.info("Starting Silver job for execution_date=%s", execution_date)

        bronze_df = self.reader.read(spark, execution_date)
        logger.info("Bronze rows: %d", bronze_df.count())
        
        silver_df = self.transformer.transform(bronze_df, execution_date)
        logger.info("Silver rows after transform: %d", silver_df.count())
        
        output_path = self.writer.write(silver_df)
        logger.info("Silver data written to %s", output_path)

        return output_path