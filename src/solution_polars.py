"""Processando os dados com Polars."""

from pathlib import Path

import polars as pl

from create_measurements import FILENAME_OUTPUT, NUM_ROWS_TO_CREATE

"""
Esse código criado por Roen Vossen, porém, alterado por Gregory Oliveira
como estudo de programação.
# Created by Koen Vossen,
# Github: https://github.com/koenvo
# Twitter/x Handle: https://twitter.com/mr_le_fox
# https://x.com/mr_le_fox/status/1741893400947839362?s=20
"""


# Tamanho do chunk baseado em 10% do total de linhas
CHUNKSIZE: int = int(NUM_ROWS_TO_CREATE * 0.1)


def create_polars_df_streaming(
    filename: Path, chunksize: int = CHUNKSIZE
) -> pl.LazyFrame:
    """
    Cria e processa um DataFrame Polars em modo de streaming.

    O método lê dados de um arquivo CSV em chunks definidos pelo parâmetro `chunksize`.
    Os dados são agrupados por estação, e são calculados os valores máximo, mínimo
    e a média de uma métrica associada.

    Parameters
    ----------
    filename : Path
        Caminho do arquivo CSV a ser processado.
    chunksize : int, optional
        Tamanho do chunk para processamento em streaming. O valor padrão é
        10% do número total de linhas (CHUNKSIZE).

    Returns
    -------
    pl.LazyFrame
        Um LazyFrame do Polars contendo os dados agregados, coletados após
        o processamento em streaming.

    Raises
    ------
    FileNotFoundError
        Caso o arquivo especificado por `filename` não seja encontrado.
    ValueError
        Caso o arquivo CSV contenha dados inconsistentes ou inválidos.

    Notes
    -----
    - O arquivo CSV deve ter um separador de campo `;` e não possuir cabeçalho.
    - As colunas esperadas no CSV são renomeadas para `station` e `measure`.
    - O schema especificado espera que `station` seja string e `measure` seja float64.
    """
    pl.Config.set_streaming_chunk_size(chunksize)

    # Lê o CSV em streaming
    df_stream = pl.scan_csv(
        filename,
        separator=";",
        has_header=False,
        new_columns=["station", "measure"],
        schema={"station": pl.String, "measure": pl.Float64},
    )

    # Processa os dados por chunks
    aggregated = (
        df_stream.group_by("station")
        .agg(
            [
                pl.col("measure").max().alias("max"),
                pl.col("measure").min().alias("min"),
                pl.col("measure").mean().alias("mean"),
            ]
        )
        .sort("station")
    )

    df = aggregated.collect(streaming=True)

    print(df)

    return df


if __name__ == "__main__":
    import time

    start_time = time.time()

    # Chama a função para criar o DataFrame processado
    df = create_polars_df_streaming(FILENAME_OUTPUT, CHUNKSIZE)
    took = time.time() - start_time

    print(f"Polars Took: {took:.2f} sec")
