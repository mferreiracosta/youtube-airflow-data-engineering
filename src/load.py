"""Módulo responsável pela carga dos dados nas zonas: bronze, silver e gold."""

import os

import pandas as pd


def save_to_csv(data: pd.DataFrame, file_name: str, save_path: str):
    """
    Salva os dados do tipo DataFrame como um arquivo CSV no diretório especificado.

    Args:
        data (pd.DataFrame): O DataFrame a ser salvo como um arquivo CSV.
        file_name (str): O nome base do arquivo CSV (com extensão).
        save_path (str): O caminho para o diretório onde o arquivo
        CSV será salvo.
    """
    save_file_path = os.path.join(save_path, file_name)

    try:
        data.to_csv(save_file_path, encoding="utf-8", index=False)
        print(f"[INFO] {file_name} saved successfully to {save_path}!")
    except Exception as e:
        print(f"[ERROR] Failed to save {file_name}. Reason: {e}")
