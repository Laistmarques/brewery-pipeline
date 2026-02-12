# from unittest.mock import MagicMock
# from src.pipeline.bronze.service import BreweryAPIService


# def test_service_calls_client_and_returns_data():

#     """
#     Testa se o serviço chama o cliente para buscar os dados e retorna o resultado esperado.
#     """

#     mock_client = MagicMock()
#     mock_client.fetch.return_value = [{"id": "1"}]

#     service = BreweryAPIService(client=mock_client)
#     result = service.fetch_all_breweries()

#     mock_client.fetch_breweries.assert_called_once()
#     assert result == [{"id": "1"}]
