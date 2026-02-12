from dataclasses import dataclass
from pyspark.sql import DataFrame

from src.pipeline.core.logging import get_logger
from src.pipeline.core.config import \
    SILVER_DIR, BREWERIES_DATASET, SILVER_FORMAT, \
    SILVER_PARTITIONS, DEFAULT_WRITE_MODE

logger = get_logger(__name__)


@dataclass(frozen=True)
class SilverBreweryWriter:

    silver_base_path: str = "data/silver"
    dataset_name: str = "breweries"
    mode: str = DEFAULT_WRITE_MODE

    def output_path(self) -> str:

        return str(SILVER_DIR / BREWERIES_DATASET)

    def write(self, df: DataFrame) -> str:

        out = self.output_path()

        if SILVER_FORMAT != "parquet":
            raise ValueError(f"Unsupported SILVER_FORMAT={SILVER_FORMAT}. Expected 'parquet'.")

        logger.info(
            "Writing Silver data | path=%s | format=%s | mode=%s | partitions=%s",
            out,
            SILVER_FORMAT,
            self.mode,
            SILVER_PARTITIONS,
        )

        # garantindo que as partições sejam sobrescritas dinamicamente, sem afetar outras partições
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        (df
         .repartition(1)
         .write
         .mode(self.mode)
         .partitionBy(*SILVER_PARTITIONS)
         .parquet(out)
        )

        logger.info("Silver write finished successfully | path=%s", out)
        return out