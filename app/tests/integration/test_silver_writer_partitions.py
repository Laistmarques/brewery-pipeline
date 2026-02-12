import os
from pyspark.sql import Row
from src.pipeline.silver.writer import SilverBreweryWriter


def test_silver_writer_creates_partition_dirs(spark, tmp_path, monkeypatch):

    """
    Teste para verificar se o escritor da camada Silver está criando as pastas de partição corretamente
    com base nas colunas de partição (ingestion_date, country e state)
    O teste cria um DataFrame de exemplo com as colunas necessárias, chama o método de escrita 
    do writer e depois checa se as pastas de partição foram criadas no caminho de
    """

    out_path = str(tmp_path / "silver_out")

    monkeypatch.setattr(SilverBreweryWriter, "output_path", lambda self: out_path)

    writer = SilverBreweryWriter()

    df = spark.createDataFrame([
        Row(
            id="1", 
            name="A", 
            brewery_type="micro", 
            city="Santos",
            longitude=0.0, 
            latitude=0.0,
            ingestion_date="2026-02-11", 
            country="BR", 
            state="SP"
        )
    ])

    writer.write(df)

    expected = os.path.join(out_path, "ingestion_date=2026-02-11", "country=BR", "state=SP")
    assert os.path.isdir(expected)