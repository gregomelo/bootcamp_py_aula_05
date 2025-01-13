"""Gravação dos resultados do processamento."""

import time
from typing import Callable, Union

from pandas import DataFrame
from polars import LazyFrame

from create_measurements import BASE_DIR, FILENAME_OUTPUT, NUM_ROWS_TO_CREATE
from solution_pandas import CHUNKSIZE, create_df_with_pandas
from solution_polars import create_polars_df_streaming

FILENAME_RESULTS = BASE_DIR / "../data/solution_results.csv"

DataFrameType = Union[DataFrame, LazyFrame]


def record_result(
    biblioteca: str,
    linhas_processadas: int,
    module_solution: Callable[..., DataFrameType],
    **kwargs,
) -> DataFrameType:
    """
    Executa uma solução de processamento, mede o tempo de execução e registra os resultados.

    Parameters
    ----------
    biblioteca : str
        Nome da biblioteca ou solução sendo utilizada.
    module_solution : Callable[..., DataFrameType]
        Função que implementa a solução a ser executada.
        A função deve aceitar argumentos variados, conforme sua implementação.
    kwargs : dict
        Argumentos adicionais a serem passados para a função de solução.

    Raises
    ------
    FileNotFoundError
        Se o arquivo de resultados não puder ser encontrado.
    Exception
        Para qualquer outro erro durante a gravação dos resultados.

    Returns
    -------
    DataFrameType
        DataFrame resultante do processamento.

    Notes
    -----
    - A função mede o tempo de execução da solução fornecida.
    - Os resultados são registrados em um arquivo especificado por `FILENAME_RESULTS` no
    formato:
        `<biblioteca>;<número de linhas>;<horário de início>;<tempo de execução (s)>`.
    - O horário de início é registrado em um formato legível por humanos (YYYY-MM-DD HH:MM:SS).
    """
    print(f"Iniciando o processamento do arquivo com {biblioteca}...")

    start_time: float = time.time()
    start_time_readable: str = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(start_time)
    )
    df = module_solution(**kwargs)
    took: float = time.time() - start_time

    print(f"Processamento concluído com: {took:.4f}s.")

    try:
        with open(FILENAME_RESULTS, "a", encoding="utf-8") as file:
            file.write(
                f"{biblioteca};{linhas_processadas};{start_time_readable};{took:.2f}\n"
            )
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
    else:
        return df


if __name__ == "__main__":

    record_result(
        "pandas",
        NUM_ROWS_TO_CREATE,
        create_df_with_pandas,
        filename=FILENAME_OUTPUT,
        total_linhas=NUM_ROWS_TO_CREATE,
        chunksize=CHUNKSIZE,
    )

    record_result(
        "polars",
        NUM_ROWS_TO_CREATE,
        create_polars_df_streaming,
        filename=FILENAME_OUTPUT,
        chunksize=CHUNKSIZE,
    )
