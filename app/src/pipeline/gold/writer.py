from dataclasses import dataclass
from pyspark.sql import DataFrame

from src.pipeline.core.logging import get_logger
from src.pipeline.core.config import (
    GOLD_DIR, 
    GOLD_BREWERIES_BY_LOCATION,
    GOLD_PARTITIONS,
    GOLD_FORMAT,
    DEFAULT_WRITE_MODE
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class GoldBreweriesWriter:
    """
    Escreve os dados transformados na camada Gold.
    """

    mode: str = DEFAULT_WRITE_MODE

    def output_path(self) -> str:
        """
        Constrói o caminho de saída para os dados Gold.

        Returns:
            str: O caminho completo onde os dados Gold serão escritos.
        """
        return str(GOLD_DIR / GOLD_BREWERIES_BY_LOCATION)
    
    def write(self, df: DataFrame) -> str:

        """
        Escreve o DataFrame transformado na camada Gold, 
        particionando por data de execução e país.
        """

        out = self.output_path()

        if GOLD_FORMAT != "parquet":
            raise ValueError(f"Unsupported GOLD_FORMAT: {GOLD_FORMAT}. Only 'parquet' is supported.")
        
        logger.info(
            "Writing Gold data | mode=%s | format=%s | path=%s | partitions=%s",
            self.mode,
            GOLD_FORMAT,
            out,
            GOLD_PARTITIONS,
        )

        # garantindo que as partições sejam sobrescritas dinamicamente, sem afetar outras partições
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        
        (df
         .repartition(1)
         .write
         .mode(self.mode)
         .partitionBy(*GOLD_PARTITIONS)
         .parquet(out)
        )

        logger.info("Gold write finished successfully")
        return out