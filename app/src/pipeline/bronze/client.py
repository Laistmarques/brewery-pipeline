import time
import requests

from src.pipeline.core.logging import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.openbrewerydb.org/v1/breweries"


class BreweryAPIClient:
    """
    Cliente responsável por realizar chamadas à API Open Brewery DB.

    Esta classe encapsula toda a lógica de comunicação HTTP,
    incluindo paginação, timeout e tentativas de retry.
    """

    def __init__(
        self,
        per_page: int = 200,
        max_retries: int = 3,
        retry_delay: int = 2,
    ):
        """
        Inicializa o cliente da API.

        Args:
            per_page (int): Quantidade de registros por página.
            max_retries (int): Número máximo de tentativas em caso de falha.
            retry_delay (int): Tempo de espera (em segundos) entre retries.
        """

        self.per_page = per_page
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def fetch_breweries(self, page: int = 1) -> list[dict]:
        """
        Realiza a requisição de uma página específica da API.

        Args:
            page (int): Número da página a ser consultada.

        Returns:
            list[dict]: Lista de cervejarias retornadas pela API.

        Raises:
            RuntimeError: Caso todas as tentativas de retry falhem.
        """

        logger.debug(
            "Fetching breweries from API | page=%s | per_page=%s",
            page,
            self.per_page,
        )

        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    BASE_URL,
                    params={"page": page, "per_page": self.per_page},
                    timeout=10,
                )

                response.raise_for_status()
                data = response.json()

                logger.info(
                    "Fetched %d records from API (page=%s)",
                    len(data),
                    page,
                )

                return data

            except requests.RequestException as e:
                logger.warning(
                    "Attempt %d/%d failed for page %s: %s",
                    attempt + 1,
                    self.max_retries,
                    page,
                    e,
                )

                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise RuntimeError(
                        f"Failed to fetch page {page} after "
                        f"{self.max_retries} attempts"
                    ) from e
