from src.pipeline.core.logging import get_logger
from .client import BreweryAPIClient

logger = get_logger(__name__)


class BreweryAPIService:

    """
    Serviço responsável por orquestrar a extração completa 
    dos dados da Open Brewery API, utilizando o cliente para realizar as requisições
    e processar os dados conforme necessário.
    """

    def __init__(self, client: BreweryAPIClient):

        """
        Inicializa o serviço de extração.
        
         Args:
            client (BreweryAPIClient): Cliente responsável pelas chamadas à API.
        """

        self.client = client

    def fetch_all_breweries(self) -> list:

        """
        Busca todas as cervejarias disponíveis na Open Brewery API,
        processando todas as paginas de resultados para garantir que todos os dados sejam extraídos.
        
        :return: Lista de dados das cervejarias extraídas da API
        """

        logger.info("Starting full extraction from Open Brewery API")

        raw_data = []
        page = 1

        while True:
            data = self.client.fetch_breweries(page)

            if not data:
                logger.info("No more data found. Finishing extraction.")
                break

            raw_data.extend(data)

            logger.debug(
                "Accumulated records: %d after page %d",
                len(raw_data),
                page,
            )
            page += 1

        logger.info("Extraction finished. Total records: %d", len(raw_data))

        return raw_data