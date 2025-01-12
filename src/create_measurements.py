"""Criação do arquivo para análise."""

import os
import random
import time
from pathlib import Path
from typing import List

# Parâmetros para Criação do Arquivo Teste
NUM_ROWS_TO_CREATE: int = 1_000_000
FILENAME_INPUT: Path = Path("../data/weather_stations.csv")
FILENAME_OUTPUT: Path = Path("../data/measurements.txt")


def check_args(file_args: List[str]) -> None:
    """
    Verifica a validade dos argumentos de entrada.

    Parameters
    ----------
    file_args : List[str]
        Lista de argumentos fornecidos pela linha de comando. O segundo argumento
        deve ser um número inteiro positivo.

    Raise
    -------
    SystemExit
        Caso a entrada não corresponda ao formato esperado ou seja inválida.

    Notes
    -----
    A função imprime informações sobre como usar o comando e encerra a execução
    do programa caso a validação falhe.
    """
    try:
        if len(file_args) != 2 or int(file_args[1]) <= 0:
            raise ValueError()
    except ValueError:
        print(
            "Usage:  create_measurements.sh <positive integer number of records to create>"
        )
        print("        You can use underscore notation for large number of records.")
        print("        For example:  1_000_000_000 for one billion")
        exit()


def build_weather_station_name_list() -> List[str]:
    """
    Extrai e deduplica os nomes das estações meteorológicas de um arquivo CSV.

    Return
    -------
    List[str]
        Uma lista contendo os nomes únicos das estações meteorológicas.

    Notas
    -----
    A função pressupõe que o arquivo `./data/weather_stations.csv` esteja presente
    e formatado corretamente.
    """
    station_names: List[str] = []
    try:
        with open(FILENAME_INPUT, "r", encoding="utf-8") as file:
            file_contents = file.read()
        for station in file_contents.splitlines():
            if "#" in station:
                next
            else:
                station_names.append(station.split(";")[0])
        return list(set(station_names))
    except FileNotFoundError:
        print(  # noqa (evitar conflito do black e do autopep8)
            "Verifique se o ambiente virtual está ativo. "
            "Para isso, use o comando 'poetry shell' antes de iniciar a execução do programa."
        )
        exit()


def convert_bytes(num: float) -> str:
    """
    Convert um tamanho em bytes para um formato legível por humanos.

    Parameters
    ----------
    num : float
        O tamanho em bytes.

    Return
    -------
    str
        O tamanho formatado como uma string legível por humanos (e.g., KiB, MiB, GiB).

    Notes
    -----
    Suporta a conversão até GiB.
    """
    for x in ["bytes", "KiB", "MiB", "GiB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
    return "Tamanho desconhecido"


def format_elapsed_time(seconds: float) -> str:
    """
    Formata um tempo em segundos para um formato legível por humanos.

    Parameters
    ----------
    seconds : float
        O tempo decorrido em segundos.

    Return
    -------
    str
        O tempo formatado como uma string legível por humanos.

    Notes
    -----
    Formata o tempo em segundos, minutos e/ou horas, dependendo da duração.
    """
    if seconds < 60:
        return f"{seconds:.3f} segundos"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutos {int(seconds)} segundos"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} horas {int(seconds)} segundos"
        else:
            return f"{int(hours)} horas {int(minutes)} minutos {int(seconds)} segundos"


def estimate_file_size(
    weather_station_names: List[str], num_rows_to_create: int
) -> str:
    """
    Estima o tamanho do arquivo de dados a ser criado.

    Parameters
    ----------
    weather_station_names : List[str]
        Lista com os nomes das estações meteorológicas.
    num_rows_to_create : int
        Número de linhas a serem criadas no arquivo de dados.

    Return
    -------
    str
        Uma string com a estimativa do tamanho do arquivo em um formato legível por humanos.

    Notes
    -----
    A estimativa é baseada em um cálculo médio do tamanho de cada registro.
    """
    max_string: float = float("-inf")
    min_string: float = float("inf")
    per_record_size: float = 0

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size: float = num_rows_to_create * per_record_size
    human_file_size: str = convert_bytes(total_file_size)

    message_return = (
        f"O tamanho estimado do arquivo é:  "
        f"{human_file_size}.\nO tamanho final será provavelmente muito menor (metade)."
    )

    return message_return


def build_test_data(weather_station_names: List[str], num_rows_to_create: int) -> None:
    """
    Gera e escreve um arquivo de dados de teste com o número solicitado de registros.

    Parameters
    ----------
    weather_station_names : List[str]
        Lista com os nomes das estações meteorológicas.
    num_rows_to_create : int
        Número de registros a serem criados no arquivo.

    Return
    -------
    None
        Não retorna nada, mas cria um arquivo `measurements.txt` com os dados gerados.

    Notes
    -----
    O arquivo é criado em lotes para melhorar a eficiência e reduzir o tempo de escrita.
    """
    start_time: float = time.time()
    coldest_temp: float = -99.9
    hottest_temp: float = 99.9
    station_names_10k_max: List[str] = random.choices(weather_station_names, k=10_000)
    batch_size: int = 10_000
    print("Criando o arquivo... isso vai demorar uns 10 minutos...")

    try:
        with open(FILENAME_OUTPUT, "w", encoding="utf-8") as file:
            for _ in range(0, num_rows_to_create // batch_size):
                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = "\n".join(
                    [
                        f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}"
                        for station in batch
                    ]
                )
                file.write(prepped_deviated_batch + "\n")
    except FileNotFoundError:
        print(  # noqa (evitar conflito do black e do autopep8)
            "Verifique se o ambiente virtual está ativo. "
            "Para isso, use o comando 'poetry shell' antes de iniciar a execução do programa."
        )
        exit()
    except Exception as e:
        print("Ocorreu um erro ao criar o arquivo:")
        print(e)
        exit()

    end_time: float = time.time()
    elapsed_time: float = end_time - start_time
    file_size: int = os.path.getsize(FILENAME_OUTPUT)
    human_file_size: str = convert_bytes(file_size)

    print(f"Arquivo escrito com sucesso: {FILENAME_OUTPUT}")
    print(f"Tamanho final:  {human_file_size}")
    print(f"Tempo decorrido: {format_elapsed_time(elapsed_time)}")


if __name__ == "__main__":
    weather_station_names: List[str] = build_weather_station_name_list()
    print(estimate_file_size(weather_station_names, NUM_ROWS_TO_CREATE))
    build_test_data(weather_station_names, NUM_ROWS_TO_CREATE)
    print("Arquivo de teste finalizado.")
