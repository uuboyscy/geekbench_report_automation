"""
Sync CPU model record on benchmark page to PostgreSQL.

Source:
    - Webpage
        - https://browser.geekbench.com/processor-benchmarks
Target:
    - PostgreSQL
        - geekbench_report.cpu_model_benchmarks
            ```sql
            -- Table Definition
            CREATE TABLE "public"."cpu_model_benchmarks" (
                "cpu_model" text,
                "frequency" text,
                "cores" int8,
                "single_core_score" int8,
                "multi_core_score" int8
            );
            ```

Run in n8n container:
Not scheduled yet.

"""

from dataclasses import asdict

import pandas as pd
from prefect import flow, task

# from utils.core.database_helper import load_df_to_pg
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
def print_df(df: pd.DataFrame) -> None:
    print(df)

@flow(name=generate_flow_name())
def sync_cpu_model_benchmarks_to_pg() -> None:
    """Sync CPU model benchmark data to PostgreSQL database."""
    benchmark_list = e_scrape_page()
    df = t_geekbench_processor_benchmark_to_df(benchmark_list)
    print_df(df)
    # load_df_to_pg(
    #     df,
    #     table_name="cpu_model_benchmarks",
    #     if_exists="replace",
    # )  # or "append" if you want to keep old data


if __name__ == "__main__":
    sync_cpu_model_benchmarks_to_pg()
