import argparse

from pipeline.core.spark_session import get_spark
from pipeline.schemas.brewery_schema import BREWERY_SCHEMA
from pipeline.core.config import DEFAULT_APP_NAME
from pipeline.core.logging import get_logger

from pipeline.bronze.run import run_extract

from pipeline.silver.reader import BronzeBreweryReader
from pipeline.silver.transformer import BrewerySilverTransformer
from pipeline.silver.writer import SilverBreweryWriter
from pipeline.silver.job import SilverBreweryJob

from pipeline.gold.reader import SilverBreweriesReader
from pipeline.gold.transformer import BreweryGoldTransformer
from pipeline.gold.writer import GoldBreweriesWriter
from pipeline.gold.job import GoldBreweryJob

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