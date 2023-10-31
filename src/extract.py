"""Módulo de extração de dados de canais do YouTube via API."""

import os
from datetime import datetime
from typing import Tuple

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import Resource, build
from loguru import logger

from .load import save_to_csv

env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
)
load_dotenv(dotenv_path=env_path)

API_KEY = os.environ.get("API_KEY")
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"


def ensure_dir(directory: str):
    """Cria o diretório caso ele não exista."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def authenticated_service() -> Resource:
    """
    Retorna um objeto Resource autenticado para interagir com o serviço da API.

    Essa função constrói e retorna um objeto Resource configurado
    para interagir com o serviço da API, utilizando as credenciais
    e a chave de API configuradas no módulo ou no ambiente.

    Returns:
        googleapiclient.discovery.Resource: Um objeto Resource
        configurado para o serviço da API.

    Raises:
        YoutubeAPIKeyError: Se a chave de API do YouTube for inválida.
        UnknownApiNameOrVersion: Se o nome do serviço ou
        a versão da API forem inválidos.

    """
    return build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)


def extract_all_channel_info_by_usename(
    service: Resource, max_results_per_page: int, **kwargs
) -> Tuple[pd.DataFrame, int]:
    """
    Obtém informações sobre canal do YouTube com base no nome de usuário.

    Esta função utiliza o serviço da API do YouTube para buscar
    informações sobre canal com base no nome de usuário fornecido.
    Ela retorna os resultados em um DataFrame, que contém informações
    detalhadas sobre o canal.

    Args:
        service (googleapiclient.discovery.Resource):
            O serviço da API do YouTube configurado usando a biblioteca
            google-api-python-client.
        **kwargs (dict):
            Um dicionário de argumentos nomeados adicionais para personalizar
            a consulta.
            Os argumentos suportados incluem:
            - part (str): As partes da informação do canal a serem recuperadas
              (Padrão: "snippet,contentDetails,statistics").
            - forUsername (str): O nome de usuário do canal a ser pesquisado
              (Padrão: "einerdtv").

    Returns:
        Tuple[pd.DataFrame, int]: Uma tupla contendo o Id do canal e um
        DataFrame contendo informações detalhadas sobre o canal.

    Raises:
        Alguma exceção que a função pode levantar, se aplicável.
    """
    try:
        response = service.channels().list(**kwargs).execute()

        id = response["items"][0]["id"]
        title = response["items"][0]["snippet"]["title"]
        description = response["items"][0]["snippet"]["description"]
        custom_url = response["items"][0]["snippet"]["customUrl"]
        publishedAt = response["items"][0]["snippet"]["publishedAt"]
        country = response["items"][0]["snippet"]["country"]
        view_count = response["items"][0]["statistics"]["viewCount"]
        subscriber_count = response["items"][0]["statistics"]["subscriberCount"]
        video_count = response["items"][0]["statistics"]["videoCount"]

        channel_info = {
            "id": id,
            "title": title,
            "description": description,
            "custom_url": custom_url,
            "publishedAt": publishedAt,
            "country": country,
            "view_count": view_count,
            "subscriber_count": subscriber_count,
            "video_count": video_count,
        }

        logger.info("Extração de informações sobre o canal concluída com sucesso.")
    except Exception as e:
        logger.error(f"Erro na extração de informações sobre o canal: {e}")

    # extract total pages of the channel
    response = (
        service.search()
        .list(
            part="snippet",
            channelId=channel_info["id"],
            maxResults=max_results_per_page,
        )
        .execute()
    )
    if "pageInfo" in response:
        total_results = response["pageInfo"]["totalResults"]
        total_pages = total_results // max_results_per_page
        if total_results % max_results_per_page > 0:
            total_pages += 1

    return pd.DataFrame([channel_info]), total_pages


def extract_all_videos_info(service: Resource, **kwargs) -> Tuple[pd.DataFrame, str]:
    """
    Obtém informações sobre vídeos da API do YouTube.

    Esta função consulta a API do YouTube para recuperar informações
    sobre vídeos com base nos parâmetros fornecidos em **kwargs.
    Ela retorna uma tupla contendo o token da próxima página
    para paginação e um DataFrame do Pandas contendo informações
    detalhadas sobre cada vídeo.

    Args:
        service (googleapiclient.discovery.Resource):
            O serviço da API do YouTube configurado usando a biblioteca
            google-api-python-client.

        **kwargs (dict):
            Um dicionário de argumentos nomeados adicionais para
            personalizar a consulta.
            Os argumentos suportados incluem:
            part (str): As partes da informação do canal a serem
              recuperadas (padrão: "id,snippet").
            channelId (str): O ID do canal do YouTube para o qual você
              deseja recuperar informações sobre os vídeos.
            type (str): O parâmetro type restringe uma consulta de pesquisa
              para recuperar apenas um determinado tipo de recurso.
            order (str): O parâmetro order especifica o método que será
              usado para ordenar recursos na resposta da API.
            pageToken (str): O token de página para paginar os resultados.
              Use-o para recuperar o próximo conjunto de vídeos. Padrão é None.

    Returns:
        Tuple[str, pd.DataFrame]: Uma tupla contendo:
            O token da próxima página para paginação. Se não houver
              mais páginas, será None.
            Um DataFrame do Pandas com informações sobre os vídeos,
              incluindo ID, título, descrição, data de publicação
              e estatísticas.

    Raises:
        Quaisquer exceções que a função possa gerar durante as
        solicitações à API.
    """
    try:
        response = service.search().list(**kwargs).execute()

        next_page_token = response.get("nextPageToken", None)

        video_list = []
        for item in response.get("items", []):
            id = item["id"]["videoId"]
            channel_id = item["snippet"]["channelId"]
            title = item["snippet"]["title"]
            description = item["snippet"]["description"]
            publishedAt = item["snippet"]["publishedAt"]

            # Use the 'videos().list' method to retrieve video statistics
            video_statistics_response = (
                service.videos().list(part="statistics", id=id).execute()
            )
            statistics = video_statistics_response["items"][0]["statistics"]
            view_count = statistics.get("viewCount", 0)
            like_count = statistics.get("likeCount", 0)
            dislike_count = statistics.get("dislikeCount", 0)
            comment_count = statistics.get("commentCount", 0)

            video_info = {
                "id": id,
                "channel_id": channel_id,
                "title": title,
                "description": description,
                "publishedAt": publishedAt,
                "view_count": view_count,
                "like_count": like_count,
                "dislike_count": dislike_count,
                "comment_count": comment_count,
            }

            video_list.append(video_info)

        logger.info(
            "Extração de informações sobre os vídeos do canal concluída com sucesso."
        )
    except Exception as e:
        video_list = []
        logger.error(f"Erro na extração de informações sobre os vídeos do canal: {e}")

    return pd.DataFrame(video_list), next_page_token


def extract_full(save_path: str, published_date: str):
    """Pipeline responsável por chamar todos os métodos de extração."""
    # Verifique se os diretórios necessários existem ou crie-os
    ensure_dir(save_path)

    service = authenticated_service()
    max_results_per_page = 50
    _datetime = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    df_channel, total_pages = extract_all_channel_info_by_usename(
        service,
        max_results_per_page=max_results_per_page,
        part="snippet,contentDetails,statistics",
        forUsername="einerdtv",
    )
    file_name = f"channel_einerd_{_datetime}.csv"
    save_to_csv(df_channel, file_name, save_path)

    df = None
    next_page_token = None
    df_videos = pd.DataFrame()
    for page_number in range(1, 50):
        df, next_page_token = extract_all_videos_info(
            service,
            part="id,snippet",
            channelId=df_channel.loc[0, "id"],
            type="video",
            order="date",
            maxResults=max_results_per_page,
            pageToken=None if page_number == 1 else next_page_token,
            publishedAfter=published_date,
        )
        df_videos = pd.concat([df_videos, df])

        if next_page_token is None:
            break
    file_name = f"videos_einerd_{_datetime}.csv"
    save_to_csv(df_videos, file_name, save_path)


if __name__ == "__main__":
    save_path = "data/bronze/"

    extract_full(save_path)
