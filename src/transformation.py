"""Módulo responsável pela transformação dos dados."""

import os
import re
from datetime import datetime
from typing import Dict, List

import pandas as pd


def ensure_dir(list_of_directory: List[str]):
    """Cria os diretório caso eles não existam."""
    for directory in list_of_directory:
        if not os.path.exists(directory):
            os.makedirs(directory)


def add_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas de qualidade ao dataframe fornecido."""
    df["_source"] = "YouTube"
    df["_datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def convert_datetime(datetime_string: str) -> str:
    """
    Converta datas para o formato 'yyyy-MM-dd HH:mm:ss'.

    Args:
        datetime_string (str): Data e hora no formato a ser convertido 'yyyy-MM-ddTH:M:SZ'.

    Return:
        str: Data e hora convertida para o padrão desejado.
    """
    return datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%SZ").strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def get_processed_files(silver_zone: str) -> Dict[str, pd.DataFrame]:
    """Obtém os arquivos processado da camada silver."""
    # Obtendo uma lista de arquivos processados .parquet
    csv_files = [f for f in os.listdir(silver_zone) if f.endswith(".parquet")]

    dict_restored_df = {}
    for idx, filename in enumerate(csv_files):
        try:
            file_path = os.path.join(silver_zone, filename)
            dict_restored_df[filename] = pd.read_parquet(file_path, engine="pyarrow")
            # remove o arquivo lido do diretório
            os.remove(file_path)
        except Exception as e:
            print(f"[ERRO] Falha ao tentar ler os arquivos .parquet: {e}")

    return dict_restored_df


def process_files(bronze_zone: str) -> Dict[str, pd.DataFrame]:
    """
    Processa os arquivos dentro do diretório fornecido e salva no diretório de destino.

    Args:
        load_path (str): diretório em que buscará o CSV.
        save_path (str): diretório em que será salvo o CSV processado.

    Returns:
        List[str]: lista de mensagens indicando quais arquivos foram processados.
    """
    # Obtendo uma lista de arquivos procesados .csv
    csv_files = [f for f in os.listdir(bronze_zone) if re.search("2023-10-27", f)]

    dict_process_df = {}
    for idx, filename in enumerate(csv_files):
        try:
            file_path = os.path.join(bronze_zone, filename)
            df = pd.read_csv(
                file_path,
                encoding="utf-8",
                encoding_errors="ignore",
                low_memory=False,
            )

            # Converte a coluna "publishedAt" para novo formato
            df["publishedAt"] = df["publishedAt"].apply(convert_datetime)

            # Adiciona colunas de qualidade, independente do arquivo
            df = add_quality_columns(df)

            dict_process_df[filename] = df

            print(f"[SUCESSO] Arquivo '{filename}' foi processado.")

        except Exception as e:
            print(f"[ERRO] Não foi possível processar o arquivo '{filename}': {e}")

    return dict_process_df


def consolidated_all_files(bronze_zone: str, silver_zone: str):
    """Consolida os arquivos em um único DataFrame."""
    dict_restored_df = get_processed_files(silver_zone)
    dict_process_df = process_files(bronze_zone)

    all_dfs = {"channel": {}, "videos": {}}

    for key, value in dict_restored_df.items():
        target_dict = (
            all_dfs["videos"] if key.startswith("videos_") else all_dfs["channel"]
        )
        target_dict[key] = value

    for key, value in dict_process_df.items():
        target_dict = (
            all_dfs["videos"] if key.startswith("videos_") else all_dfs["channel"]
        )
        target_dict[key] = value

    # Consolidando todos DataFrames em um único
    consolidated_channel_df = pd.concat(all_dfs["channel"].values(), ignore_index=True)
    consolidated_video_df = pd.concat(all_dfs["videos"].values(), ignore_index=True)

    for outer_key, inner_dict in all_dfs.items():
        if outer_key == "channel":
            filename = str(next(iter(inner_dict.keys())))
            base_filename = f"{'_'.join(filename.split('_')[:-1])}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            save_file_path = os.path.join(save_path, f"{base_filename}.parquet")

            consolidated_channel_df.to_parquet(
                save_file_path, engine="pyarrow", index=False
            )
        else:
            filename = str(next(iter(inner_dict.keys())))
            base_filename = f"{'_'.join(filename.split('_')[:-1])}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            save_file_path = os.path.join(save_path, f"{base_filename}.parquet")

            consolidated_video_df.to_parquet(
                save_file_path, engine="pyarrow", index=False
            )


def transformation_full(bronze_zone: str, silver_zone: str) -> List[str]:
    """
    Função completa para o processo de transformação dos arquivos CSV.

    Args:
        load_path (str): diretório em que buscará o CSV.
        save_path (str): diretório em que será salvo o CSV processado.

    Returns:
        List[str]: lista de mensagens indicando quais arquivos foram processados.
    """
    ensure_dir([bronze_zone, silver_zone])
    return consolidated_all_files(bronze_zone, silver_zone)


if __name__ == "__main__":
    directory_path = "data/bronze"
    save_path = "data/silver"
    transformation_full(directory_path, save_path)
