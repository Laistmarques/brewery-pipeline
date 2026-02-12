from pyspark.sql import Row
from src.pipeline.gold.transformer import BreweryGoldTransformer


def test_gold_transform_aggregates_and_ranks(spark):

    """
    Teste para verificar se o transformador da camada Gold está 
    agregando corretamente o número de cervejarias por estado,
    calculando o total por país/dia, a porcentagem do total e aplicando o ranking corretamente
    """

    df = spark.createDataFrame(
    [
        # country BR, date D, state SP tem 2 breweries
        Row(ingestion_date="2026-02-11", country="BR", state="SP", id="1", city="A", brewery_type="micro"),
        Row(ingestion_date="2026-02-11", country="BR", state="SP", id="2", city="B", brewery_type="micro"),
        # state RJ tem 1 brewery
        Row(ingestion_date="2026-02-11", country="BR", state="RJ", id="3", city="C", brewery_type="brewpub"),
    ])

    out = BreweryGoldTransformer(TOP_N=5).transform(df)

    # aqui devem existir 2 linhas (SP e RJ)
    rows = {r["state"]: r for r in out.collect()}
    assert set(rows.keys()) == {"SP", "RJ"}

    assert rows["SP"]["brewery_count"] == 2
    assert rows["RJ"]["brewery_count"] == 1

    # total por país/dia = 3
    assert rows["SP"]["country_brewery_total"] == 3
    assert abs(rows["SP"]["pct_country_total"] - (2/3)) < 1e-9
    assert rows["SP"]["rank_state_in_country"] == 1
    assert rows["RJ"]["rank_state_in_country"] == 2


def test_gold_transform_applies_top_n(spark):

    """
    Teste para verificar se o transformador da camada Gold está aplicando 
    corretamente a lógica de TOP N estados por país/dia
    """

    # 3 estados, TOP_N=2 => deve sobrar 2
    df = spark.createDataFrame([
        Row(ingestion_date="2026-02-11", country="BR", state="A", id="1", city="X", brewery_type="micro"),
        Row(ingestion_date="2026-02-11", country="BR", state="B", id="2", city="X", brewery_type="micro"),
        Row(ingestion_date="2026-02-11", country="BR", state="C", id="3", city="X", brewery_type="micro"),
    ])

    out = BreweryGoldTransformer(TOP_N=2).transform(df)
    assert out.count() == 2
