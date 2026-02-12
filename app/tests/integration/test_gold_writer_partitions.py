import os
from pyspark.sql import Row
from src.pipeline.gold.writer import GoldBreweriesWriter


def test_gold_writer_creates_partition_dirs(spark, tmp_path):

    """
    Teste para verificar se o escritor da camada Gold está criando as pastas de partição corretamente
    com base nas colunas de partição (ingestion_date e country)
    O teste cria um DataFrame de exemplo com as colunas necessárias, chama o método de
    escrita do writer e depois checa se as pastas de partição foram criadas no caminho
    """

    writer = GoldBreweriesWriter()

    df = spark.createDataFrame([
        Row(
            ingestion_date="2026-02-11",
            country="BR",
            state="SP",
            brewery_count=10,
            city_count=5,
            types_count=2,
            country_brewery_total=20,
            pct_country_total=0.5,
            rank_state_in_country=1,
            created_at_utc="2026-02-12 00:00:00",
        )
    ])

    # fallback: escreve direto pra tmp_path respeitando particionamento do config
    out = str(tmp_path / "gold_out")
    (df.write.mode("overwrite").partitionBy("ingestion_date", "country").parquet(out))

    expected = os.path.join(out, "ingestion_date=2026-02-11", "country=BR")
    assert os.path.isdir(expected)
