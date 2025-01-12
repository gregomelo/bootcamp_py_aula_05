"""Processando os dados com Pandas paralelizado."""

from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import List

import pandas as pd
from tqdm import tqdm  # importa o tqdm para barra de progresso

from create_measurements import NUM_ROWS_TO_CREATE

CONCURRENCY: int = cpu_count()

total_linhas: int = NUM_ROWS_TO_CREATE  # Total de linhas conhecido
chunksize: int = int(total_linhas * 0.1)  # Define o tamanho do chunk
filename: Path = Path(
    "../data/measurements.txt"  # Certifique-se de que este é o caminho correto para o arquivo
)


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
    filename: Path, total_linhas: int, chunksize: int = chunksize
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
        'station', 'min', 'max' e 'mean'.

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

    return final_aggregated_df


if __name__ == "__main__":
    """
    Bloco principal que executa o processamento do arquivo.

    - Define o número de linhas e tamanho do chunk.
    - Processa o arquivo com `create_df_with_pandas`.
    - Exibe o tempo total de processamento e as primeiras linhas do DataFrame resultante.

    Notes
    -----
    Este bloco deve ser executado apenas quando o script é chamado diretamente.
    """
    import time

    print("Iniciando o processamento do arquivo.")
    start_time: float = time.time()
    df: pd.DataFrame = create_df_with_pandas(filename, total_linhas, chunksize)
    took: float = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")
