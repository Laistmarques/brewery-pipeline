from unittest.mock import patch

@patch("pipeline.main.run_bronze")
def test_main_calls_bronze(mock_run):

    """
    Testa se a função main chama corretamente a função run_bronze com os parâmetros esperados.
    """

    from pipeline.main import main
    main("bronze", "2026-02-11")
    mock_run.assert_called_once()
