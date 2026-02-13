import logging
import sys


def get_logger(
    name: str, 
    level: int = logging.INFO
) -> logging.Logger:
    
    """
    Cria e retorna um logger padronizado para o pipeline.

    Esta função centraliza a configuração de logging da aplicação,
    garantindo um formato consistente de logs em todos os módulos
    da pipeline (Bronze, Silver, Gold).

    Args:
        name (str): Nome do logger, geralmente `__name__`,
            permitindo identificar o módulo de origem do log.
        level (int, optional): Nível de log a ser utilizado.
            Padrão: logging.INFO.

    Returns:
        logging.Logger: Logger configurado e pronto para uso.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
