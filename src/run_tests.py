"""Programa para teste com diferentes quantidade de linha."""

from typing import List

from create_measurements import (
    FILENAME_OUTPUT,
    build_test_data,
    build_weather_station_name_list,
    estimate_file_size,
)
from record_result import record_result
from solution_pandas import CHUNKSIZE, create_df_with_pandas
from solution_polars import create_polars_df_streaming

quantidade_linhas = [100_000 * 10**x for x in range(1, 3)]

if __name__ == "__main__":

    weather_station_names: List[str] = build_weather_station_name_list()
    for quantidade_linha in quantidade_linhas:
        # Gerando arquivos teste
        print(estimate_file_size(weather_station_names, quantidade_linha))
        build_test_data(weather_station_names, quantidade_linha)
        print("Arquivo de teste finalizado...\n\n")
        print(f"Iniciando testes com {quantidade_linha:,}...\n\n")
        record_result(
            "pandas",
            quantidade_linha,
            create_df_with_pandas,
            filename=FILENAME_OUTPUT,
            total_linhas=quantidade_linha,
            chunksize=CHUNKSIZE,
        )

        record_result(
            "polars",
            quantidade_linha,
            create_polars_df_streaming,
            filename=FILENAME_OUTPUT,
            chunksize=CHUNKSIZE,
        )
