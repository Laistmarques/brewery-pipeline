from unittest.mock import MagicMock
from src.pipeline.silver.job import SilverBreweryJob


def test_silver_job_orchestrates_reader_transformer_writer():
    
    spark = MagicMock()

    reader = MagicMock()
    transformer = MagicMock()
    writer = MagicMock()

    bronze_df = MagicMock()
    bronze_df.count.return_value = 100

    silver_df = MagicMock()
    silver_df.count.return_value = 80

    reader.read.return_value = bronze_df
    transformer.transform.return_value = silver_df
    writer.write.return_value = "data/silver/out"

    job = SilverBreweryJob(reader=reader, transformer=transformer, writer=writer)

    out = job.run(spark, "2026-02-11")

    reader.read.assert_called_once_with(spark, "2026-02-11")
    transformer.transform.assert_called_once_with(bronze_df, "2026-02-11")
    writer.write.assert_called_once_with(silver_df)
    assert out == "data/silver/out"
