from pyspark.sql import SparkSession

from pipeline.core.config import DEFAULT_APP_NAME


def get_spark(
    app_name: str = DEFAULT_APP_NAME
) -> SparkSession:

    return (
        SparkSession.builder
        .config("spark.speculation", "false") # Evita reexecução de tarefas lentas, o que pode causar problemas de concorrência com arquivos temporários
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") # Evita criação de arquivos _SUCCESS e part-00000.crc
        .appName(app_name)
        .getOrCreate()
    )