import argparse

from src.pipeline.core.spark_session import get_spark
from src.pipeline.schemas.brewery_schema import BREWERY_SCHEMA
from src.pipeline.core.config import DEFAULT_APP_NAME
from src.pipeline.core.logging import get_logger

from src.pipeline.bronze.run import run_extract

from src.pipeline.silver.reader import BronzeBreweryReader
from src.pipeline.silver.transformer import BrewerySilverTransformer
from src.pipeline.silver.writer import SilverBreweryWriter
from src.pipeline.silver.job import SilverBreweryJob

from src.pipeline.gold.reader import SilverBreweriesReader
from src.pipeline.gold.transformer import BreweryGoldTransformer
from src.pipeline.gold.writer import GoldBreweriesWriter
from src.pipeline.gold.job import GoldBreweryJob

logger = get_logger(__name__)


def run_bronze(execution_date: str):
    return run_extract(execution_date)


def run_silver(execution_date: str) -> str:

    spark = get_spark(app_name=DEFAULT_APP_NAME)

    job = SilverBreweryJob(
        reader=BronzeBreweryReader(schema=BREWERY_SCHEMA),
        transformer=BrewerySilverTransformer(),
        writer=SilverBreweryWriter(),
    )
    return job.run(spark, execution_date)

def run_gold(execution_date: str) -> str:
    spark = get_spark(app_name=DEFAULT_APP_NAME)

    job = GoldBreweryJob(
        reader=SilverBreweriesReader(),
        transformer=BreweryGoldTransformer(),
        writer=GoldBreweriesWriter(),
    )
    return job.run(spark, execution_date)


def main(stage: str, execution_date: str):

    logger.info("Pipeline start | stage=%s | execution_date=%s", stage, execution_date)

    if stage in ("bronze", "all"):
        run_bronze(execution_date)
    if stage in ("silver", "all"):
        run_silver(execution_date)
    if stage in ("gold", "all"):
        run_gold(execution_date)

    logger.info("Pipeline finished | stage=%s | execution_date=%s", stage, execution_date)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["bronze", "silver", "gold", "all"], default="all")
    parser.add_argument("--date", required=True)
    
    args = parser.parse_args()
    main(args.stage, args.date)