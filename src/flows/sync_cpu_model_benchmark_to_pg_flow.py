"""
Sync CPU model record on benchmark page to BigQuery.

Source:
    - Webpage
        - https://browser.geekbench.com/processor-benchmarks
Target:
    - BigQuery
        - geekbench_report.cpu_model_benchmarks
        - Schema:
            ```sql
            CREATE TABLE `geekbench_report.cpu_model_benchmarks` (
                cpu_model STRING,
                frequency STRING,
                cores INT64,
                single_core_score INT64,
                multi_core_score INT64
            );
            ```

Run on Prefect Server.
"""

from dataclasses import asdict

import pandas as pd
from google.cloud.bigquery import Client as BigQueryClient
from prefect import flow, task

from utils.bigquery_utility import load_dataframe_to_bigquery
from utils.core.geekbench.geekbench_processor_benchmark_scraper import (
    GeekbenchProcessorBenchmark,
    scrape_page,
)
from utils.prefect_utility import generate_flow_name


@task
def e_scrape_page() -> list[GeekbenchProcessorBenchmark]:
    return scrape_page()

@task
def t_geekbench_processor_benchmark_to_df(
    benchmark_list: list[GeekbenchProcessorBenchmark],
) -> pd.DataFrame:
    return pd.DataFrame([asdict(b) for b in benchmark_list])

@task
def l_load_df_to_bq(df: pd.DataFrame) -> None:
    load_dataframe_to_bigquery(
        bigquery_client=BigQueryClient(),
        dataframe=df,
        destination="geekbench_report.cpu_model_benchmarks",
        write_disposition="WRITE_TRUNCATE",
    )

@flow(name=generate_flow_name())
def sync_cpu_model_benchmarks_to_pg() -> None:
    """Sync CPU model benchmark data to PostgreSQL database."""
    benchmark_list = e_scrape_page()
    df = t_geekbench_processor_benchmark_to_df(benchmark_list)
    l_load_df_to_bq(df)


if __name__ == "__main__":
    sync_cpu_model_benchmarks_to_pg()
