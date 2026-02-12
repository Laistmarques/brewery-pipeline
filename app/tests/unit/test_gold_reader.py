def test_gold_reader_reads_filtered_date(spark, tmp_path):

    from src.pipeline.gold.reader import SilverBreweriesReader
    from pyspark.sql import Row

    base = str(tmp_path / "silver")

    df = spark.createDataFrame([
        Row(ingestion_date="2026-02-11", country="BR", state="SP", id="1", brewery_type="micro"),
        Row(ingestion_date="2026-02-12", country="BR", state="SP", id="2", brewery_type="micro"),
    ])

    df.write.mode("overwrite").partitionBy("ingestion_date", "country", "state").parquet(base)

    reader = SilverBreweriesReader(silver_dir=base)
    result = reader.read(spark, "2026-02-11")

    assert result.count() == 1
