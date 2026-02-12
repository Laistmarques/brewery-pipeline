from pyspark.sql import SparkSession

from src.pipeline.core.config import DEFAULT_APP_NAME


def get_spark(
    app_name: str = DEFAULT_APP_NAME
) -> SparkSession:

    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )