"""Processando os dados com datatable."""

from multiprocessing import cpu_count
from pathlib import Path

import datatable as dt

from create_measurements import FILENAME_OUTPUT

CONCURRENCY: int = cpu_count()


def create_df_with_datatable(filename: Path) -> dt.Frame:
    """
    Cria e processa um DataFrame com a biblioteca datatable.

    O método lê um arquivo CSV em um DataFrame do datatable e realiza operações de
    agregação como cálculo de mínimo, máximo e média para os valores da coluna
    `measure`, agrupados pela coluna `station`.

    Parameters
    ----------
    filename : Path
        Caminho para o arquivo CSV a ser processado. O arquivo deve conter as colunas
        `station` e `measure`.

    Returns
    -------
    dt.Frame
        Um DataFrame do datatable contendo os valores agregados:
        - `station`: Chave de agrupamento.
        - `min`: Valor mínimo da métrica.
        - `max`: Valor máximo da métrica.
        - `mean`: Média da métrica.

    Raises
    ------
    FileNotFoundError
        Se o arquivo especificado por `filename` não existir.
    ValueError
        Caso o arquivo não contenha as colunas esperadas ou esteja corrompido.

    Notes
    -----
    - O processamento utiliza múltiplos threads para melhorar a performance, de acordo
      com o número de CPUs disponíveis.
    - A biblioteca datatable é otimizada para lidar com grandes volumes de dados.
    """
    df: dt.Frame = dt.fread(
        file=filename, nthreads=CONCURRENCY, columns=["station", "measure"]
    )

    final_aggregated_df: dt.Frame = df[
        :,
        {
            "min": dt.min(dt.f.measure),
            "max": dt.max(dt.f.measure),
            "mean": dt.mean(dt.f.measure),
        },
        dt.by("station"),
    ]

    print(final_aggregated_df)

    return final_aggregated_df


if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")

    start_time: float = time.time()
    df: dt.Frame = create_df_with_datatable(filename=FILENAME_OUTPUT)
    took: float = time.time() - start_time

    print(f"Datatable demorou: {took:.4f} sec")
