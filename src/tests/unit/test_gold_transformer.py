from pyspark.sql import Row
from pipeline.gold.transformer import BreweryGoldTransformer


def test_gold_transformer_aggregates_by_type_country_state_city(spark):

    df = spark.createDataFrame([
        Row(
            id="1",
            brewery_type="micro",
            country="BR",
            state="SP",
            city="Sao Paulo",
            ingestion_date="2026-02-12",
        ),
        Row(
            id="2",
            brewery_type="micro",
            country="BR",
            state="SP",
            city="Sao Paulo",
            ingestion_date="2026-02-12",
        ),

        Row(
            id="2",
            brewery_type="micro",
            country="BR",
            state="SP",
            city="Sao Paulo",
            ingestion_date="2026-02-12",
        ),

        Row(
            id="3",
            brewery_type="brewpub",
            country="BR",
            state="SP",
            city="Campinas",
            ingestion_date="2026-02-12",
        ),
    ])

    out = BreweryGoldTransformer().transform(df)

    # Colunas esperadas
    cols = set(out.columns)
    assert {
        "brewery_type", 
        "country", 
        "state", 
        "city", 
        "num_breweries", 
        "num_city", 
        "created_at_utc"
    }.issubset(cols)

    rows = out.collect()
    assert len(rows) == 2  # dois grupos

    first = rows[0]
    assert first["brewery_type"] == "micro"
    assert first["country"] == "BR"
    assert first["state"] == "SP"
    assert first["city"] == "Sao Paulo"
    assert first["num_breweries"] == 2  # ids distintos: 1 e 2
    assert first["num_city"] == 1       # city está no groupBy, então sempre 1 por grupo
    assert first["created_at_utc"] is not None

    # Segundo grupo
    second = rows[1]
    assert second["brewery_type"] == "brewpub"
    assert second["city"] == "Campinas"
    assert second["num_breweries"] == 1
    assert second["num_city"] == 1
    assert second["created_at_utc"] is not None
