from pyspark.sql.types import *

BREWERY_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("brewery_type", StringType()),
    StructField("city", StringType()),
    StructField("state_province", StringType()),
    StructField("country", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
])
