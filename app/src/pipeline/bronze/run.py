import json 

from datetime import datetime, timezone
from pathlib import Path

from .client import BreweryAPIClient
from .service import BreweryAPIService

from src.pipeline.core.logging import get_logger
from src.pipeline.core.config import BRONZE_DIR, BREWERIES_DATASET, BRONZE_FORMAT

logger = get_logger(__name__)


def persist_bronze_data(
    raw_data: list,
    ingestion_date: str,
    base_path: str = "data/bronze",
    dataset_name: str = "breweries",
    overwrite: bool = True,
) -> str:
    """
    Persiste os dados brutos da camada Bronze em formato JSON.

    Args:
        raw_data (list): Lista de registros brutos extraídos da API.
        ingestion_date (str): Data de ingestão no formato YYYY-MM-DD.
        base_path (str): Caminho base da camada Bronze.
        dataset_name (str): Nome lógico do dataset a ser salvo.
        overwrite: Se True, sobrescreve a partição do dia.
                   Se False, não reprocessa caso já exista.

    Returns:
        str: Caminho completo do arquivo salvo.

    Raises:
        ValueError: Caso a lista de dados esteja vazia.
    """

    # if not raw_data:
    #     raise ValueError("None data to persist. The raw_data list is empty.")
    
    # if BRONZE_FORMAT != "json":
    #     raise ValueError(f"Unsupported BRONZE_FORMAT={BRONZE_FORMAT}. Expected 'json'.")

    output_dir = (
        Path(BRONZE_DIR)
        / BREWERIES_DATASET
        / f"{BRONZE_PARTITION_KEY}={ingestion_date}"
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{BREWERIES_DATASET}.json"

    if file_path.exists() and not overwrite:
        logger.warning(
            "File already exists and overwrite is set to False. \
             Skipping persistence | path=%s",
            file_path,
        )
        return str(file_path)

    tmp_path = output_dir / f".{BREWERIES_DATASET}.{ingestion_date}.tmp.json"

    logger.info(
        "Persisting data to Bronze | records=%d | path=%s",
        len(raw_data),
        file_path,
    )

    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=4)

    tmp_path.replace(file_path)

    logger.info("Data successfully persisted to Bronze | path=%s", file_path)

    return str(file_path)


def run_extract(ingestion_date: str) -> str:
    """
    Executa o fluxo completo de extração da Open Brewery API,
    desde a coleta dos dados até a persistência na camada Bronze.

    Args:
        execution_date (str): Data de execução no formato YYYY-MM-DD.

    Returns:
        str: Caminho do arquivo onde os dados foram salvos.
    """

    logger.info("Starting Bronze extraction | ingestion_date=%s", ingestion_date)
                
    client = BreweryAPIClient()
    service = BreweryAPIService(client)

    raw_data = service.fetch_all_breweries()

    output_path = persist_bronze_data(
        raw_data=raw_data,
        ingestion_date=ingestion_date,
    )

    logger.info(
        "Bronze extraction finished successfully | records=%d | path=%s",
        len(raw_data),
        output_path,
    )

    return output_path


if __name__ == "__main__":

    execution_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        output_file = run_extract(execution_date)
        print(f"Data successfully extracted and saved to: {output_file}")
    except Exception as e:
        print(f"An error occurred during extraction: {e}")