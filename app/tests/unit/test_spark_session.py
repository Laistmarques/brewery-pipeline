from src.pipeline.core.spark_session import get_spark


def test_get_spark_creates_session():

    spark = get_spark("test-session")
    assert spark.sparkContext.appName == "brewery-pipeline-tests"
