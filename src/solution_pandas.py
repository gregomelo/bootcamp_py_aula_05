"""Processando os dados com Pandas paralelizado."""

from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm

from create_measurements import FILENAME_OUTPUT, NUM_ROWS_TO_CREATE

CONCURRENCY: int = cpu_count()

CHUNKSIZE: int = int(NUM_ROWS_TO_CREATE * 0.1)


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Processa um chunk de dados, agregando as medições.

    Parameters
    ----------
    chunk : pd.DataFrame
        O chunk de dados a ser processado.

    Returns
    -------
    pd.DataFrame
        Um DataFrame com os resultados agregados, contendo as colunas
        'station', 'min', 'max' e 'mean'.
    """
    aggregated = (
        chunk.groupby("station")["measure"].agg(["min", "max", "mean"]).reset_index()
    )
    return aggregated


def create_df_with_pandas(
    filename: Path, total_linhas: int, chunksize: int = CHUNKSIZE
) -> pd.DataFrame:
    """
    Processa o arquivo em chunks, aplicando agregação paralelizada.

    Parameters
    ----------
    filename : Path
        O caminho para o arquivo de entrada.
    total_linhas : int
        O número total de linhas no arquivo.
    chunksize : int, optional
        O tamanho de cada chunk a ser processado (padrão é 10% do total).

    Returns
    -------
    pd.DataFrame
        Um DataFrame final com as medições agregadas, contendo as colunas
        'station', 'min', 'max' e 'mean'. Além disso, as primeiras linhas
        do DataFrame são impressas.

    Notes
    -----
    - O arquivo é lido em chunks para evitar sobrecarga de memória.
    - O processamento é paralelizado para melhorar a eficiência.
    - Um progresso visual é exibido usando `tqdm`.
    """
    total_chunks: int = total_linhas // chunksize + (
        1 if total_linhas % chunksize else 0
    )
    results: List[pd.DataFrame] = []

    with pd.read_csv(
        filename,
        sep=";",
        header=None,
        names=["station", "measure"],
        chunksize=chunksize,
    ) as reader:
        # Envolvendo o iterador com tqdm para visualizar o progresso
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processando"):
                # Processa cada chunk em paralelo
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df: pd.DataFrame = pd.concat(results, ignore_index=True)

    final_aggregated_df: pd.DataFrame = (
        final_df.groupby("station")
        .agg({"min": "min", "max": "max", "mean": "mean"})
        .reset_index()
        .sort_values("station")
    )

    print(final_aggregated_df.head())

    return final_aggregated_df


if __name__ == "__main__":

    import time

    print("Iniciando o processamento do arquivo.")
    start_time: float = time.time()
    df: pd.DataFrame = create_df_with_pandas(
        FILENAME_OUTPUT, NUM_ROWS_TO_CREATE, CHUNKSIZE
    )
    took: float = time.time() - start_time

    print(f"Processing took: {took:.2f} sec")
