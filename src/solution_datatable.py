"""Processando os dados com datatable."""

from multiprocessing import cpu_count
from pathlib import Path

import datatable as dt

from create_measurements import FILENAME_OUTPUT, NUM_ROWS_TO_CREATE

CONCURRENCY: int = cpu_count()

CHUNKSIZE: int = int(NUM_ROWS_TO_CREATE * 0.1)


def create_df_with_datatable(
    filename: Path, total_linhas: int, chunksize: int = CHUNKSIZE
) -> dt.Frame:
    """
    Cria e processa um DataFrame com a biblioteca datatable.

    O método lê um arquivo CSV em chunks para evitar sobrecarga de memória e realiza
    operações de agregação como cálculo de mínimo, máximo, soma e contagem para os
    valores da coluna `measure`, agrupados pela coluna `station`. Após processar
    todos os chunks, os resultados parciais são combinados para calcular os agregados
    globais, incluindo a média.

    Parameters
    ----------
    filename : Path
        Caminho para o arquivo CSV a ser processado. O arquivo deve conter as colunas
        `station` e `measure`.
    total_linhas : int
        Número total de linhas no arquivo CSV. Usado para controlar o loop de chunks.
    chunksize : int, optional
        Tamanho do chunk para processamento. O padrão é definido como 10% do total de
        linhas.

    Returns
    -------
    dt.Frame
        Um DataFrame do datatable contendo os valores agregados globais:
        - `station`: Chave de agrupamento.
        - `min`: Valor mínimo da métrica entre todos os chunks.
        - `max`: Valor máximo da métrica entre todos os chunks.
        - `mean`: Média global calculada a partir da soma e da contagem entre todos os chunks.

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
    - A soma e a contagem são utilizadas para calcular a média global corretamente.
    - O uso de chunks permite processar grandes volumes de dados sem sobrecarregar a memória.
    """
    parcial_data: list = []
    rows_to_skip: int = 0
    while rows_to_skip < total_linhas:
        # Lendo o arquivo em chunks
        df: dt.Frame = dt.fread(
            file=filename,
            nthreads=CONCURRENCY,
            columns=["station", "measure"],
            skip_to_line=rows_to_skip,
            max_nrows=chunksize,
        )
        # Caso tenhamos linhas, processamentos o chunk
        if df.nrows > 0:
            df_parcial_aggregated: dt.Frame = df[
                :,
                {
                    "min": dt.min(dt.f.measure),
                    "max": dt.max(dt.f.measure),
                    "sum": dt.sum(dt.f.measure),
                    "count": dt.count(),
                },
                dt.by("station"),
            ]

            parcial_data.append(df_parcial_aggregated)

            # Combinando os resultados parciais para reduzir a memória utilizada
            df_process: dt.Frame = dt.rbind(parcial_data, force=True)

            # Processando os resultados parciais
            df_process = df_process[
                :,
                {
                    "min": dt.min(dt.f.min),
                    "max": dt.max(dt.f.max),
                    "sum": dt.sum(dt.f.sum),
                    "count": dt.sum(dt.f.count),
                },
                dt.by("station"),
            ]

            # Preparando os dados para a próxima iteração
            parcial_data = [df_process]

        rows_to_skip = rows_to_skip + chunksize

    final_aggregated_df = dt.Frame = df_process[
        :,
        {
            "min": dt.min(dt.f.min),
            "max": dt.max(dt.f.max),
            "sum": dt.sum(dt.f.sum),
            "count": dt.sum(dt.f.count),
        },
        dt.by("station"),
    ]

    final_aggregated_df = final_aggregated_df[
        :,
        {
            "station": dt.f.station,
            "min": dt.f.min,
            "max": dt.f.max,
            "mean": dt.f.sum / dt.f.count,
        },
    ]

    print(final_aggregated_df)

    return final_aggregated_df


if __name__ == "__main__":
    import time

    print("Iniciando o processamento do arquivo.")

    start_time: float = time.time()
    df: dt.Frame = create_df_with_datatable(
        filename=FILENAME_OUTPUT, total_linhas=NUM_ROWS_TO_CREATE, chunksize=CHUNKSIZE
    )
    took: float = time.time() - start_time

    print(f"Datatable demorou: {took:.4f} sec")
