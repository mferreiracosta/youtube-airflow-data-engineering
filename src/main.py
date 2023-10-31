"""Módulo responsável pela coordenadação das etapas de extração e transformação dos dados."""

import sys
from datetime import datetime

from loguru import logger

from .extract import extract_full
from .transformation import transformation_full


def main():
    """Função principal que coordena a execução das etapas de extração e transformação."""
    # Configuração do Loguru
    log_file_path = "logs/pipeline.log"

    # Remova os manipuladores padrão
    logger.remove()

    # Adicione seus próprios manipuladores com um novo formato de data e hora
    logger_format = "<green>{time:YYYY-MM-DDTHH:mm:ss}</green> <level>{message}</level>"

    logger.add(sys.stderr, colorize=True, format=logger_format)
    logger.add(
        log_file_path,
        rotation="1 week",
        retention="1 month",
        level="INFO",
        format=logger_format,
    )

    # Parâmetros
    # "2023-10-28T00:00:00Z"
    published_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    bronze_zone = "data/bronze/"
    silver_zone = "data/silver/"

    logger.info("Iniciando a extração de dados...")

    try:
        # Executando a etapa de extração
        extract_full(bronze_zone, published_date)
        logger.info("Extração de dados concluída com sucesso.")
    except Exception as e:
        logger.info(f"Erro ocorrido durante a extração de dados: {e}")

    logger.info("Iniciando a transformação de dados...")

    try:
        # Executando a etapa de transformação
        transformation_full(bronze_zone, silver_zone)
        logger.info("Transformação de dados concluída com sucesso.")
    except Exception as e:
        logger.info(f"Erro ocorrido durante a transformação de dados: {e}")

    logger.info("Pipeline concluído.")


if __name__ == "__main__":
    main()
