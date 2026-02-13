import pytest
from pyspark.sql import Row
from pipeline.silver.transformer import BrewerySilverTransformer


def test_silver_transform_adds_ingestion_date_and_state(spark):

    """
    Teste para verificar se o transformador da camada Silver está adicionando a coluna de data de ingestão
    e renomeando a coluna de estado corretamente
    """

    df = spark.createDataFrame([
        Row(
            id="1",
            name="A",
            brewery_type="micro",
            city="Santos",
            state_province="SP",
            country="Brazil",
            longitude=-46.6,
            latitude=-23.5,
        )
    ])

    out = BrewerySilverTransformer().transform(df, "2026-02-11")

    cols = set(out.columns)
    assert "state" in cols
    assert "state_province" not in cols
    assert "ingestion_date" in cols

    row = out.first()
    assert row["state"] == "SP"
    assert row["ingestion_date"] == "2026-02-11"


def test_silver_transform_filters_null_country_or_state(spark):

    """
    Teste para verificar se o transformador da camada Silver está filtrando corretamente as linhas que possuem
    colunas de país ou estado nulas
    """

    df = spark.createDataFrame(
    [
        Row(id="1", name="A", brewery_type="micro", city="Santos",
            state_province=None, country="Brazil", longitude=0.0, latitude=0.0),
        Row(id="2", name="B", brewery_type="micro", city="São Paulo",
            state_province="SP", country=None, longitude=0.0, latitude=0.0),
        Row(id="3", name="C", brewery_type="micro", city="Paraty",
            state_province="RJ", country="Brazil", longitude=0.0, latitude=0.0),
    ])

    out = BrewerySilverTransformer().transform(df, "2026-02-11")
    assert out.count() == 1
    assert out.first()["id"] == "3"


def test_silver_transform_raises_if_missing_columns(spark):

    """
    Teste para verificar se o transformador da camada Silver 
    está levantando um erro quando as colunas obrigatórias
    estão ausentes no DataFrame de entrada
    """

    df = spark.createDataFrame([Row(id="1")])  # tirei algumas colunas obrigatórias

    with pytest.raises(ValueError):
        BrewerySilverTransformer().transform(df, "2026-02-11")
