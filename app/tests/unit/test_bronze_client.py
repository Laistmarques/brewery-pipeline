from unittest.mock import patch, MagicMock
from src.pipeline.bronze.client import BreweryAPIClient


@patch("src.pipeline.bronze.client.requests.get")
def test_client_calls_api(mock_get):

    """
    Testa se o cliente faz a chamada correta para a API e retorna os dados esperados.
    """

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": "1"}]
    mock_get.return_value = mock_response

    client = BreweryAPIClient()
    result = client.fetch_breweries()

    mock_get.assert_called_once()
    assert result == [{"id": "1"}]
