"""
Sync CPU model details to BigQuery.

SQL for creating table `cpu_model_details` in BigQuery:
```sql
CREATE TABLE `geekbench_report.cpu_model_details` (
    cpu_result_id INT64,
    title STRING,
    upload_date DATETIME,
    views INT64,
    cpu_model_id INT64,
    cpu_codename STRING,
    single_core_score INT64,
    multi_core_score INT64,
    system_info STRUCT<
        `Operating System` STRING,
        `Model` STRING,
        `Motherboard` STRING,
        `Power Plan` STRING,
        `Model ID` STRING
    >,
    cpu_info STRUCT<
        `Name` STRING,
        `Topology` STRING,
        `Identifier` STRING,
        `Base Frequency` STRING,
        `Maximum Frequency` STRING,
        `L1 Instruction Cache` STRING,
        `L1 Data Cache` STRING,
        `L2 Cache` STRING,
        `L3 Cache` STRING,
        `Package` STRING,
        `Codename` STRING,
        `Instruction Sets` STRING,
        `Cluster 1` STRING,
        `Cluster 2` STRING
    >,
    memory_info STRUCT<
        `Size` STRING,
        `Type` STRING,
        `Frequency` STRING,
        `Channels` STRING
    >,
    single_core_benchmarks STRUCT<
        `File Compression` STRUCT<score STRING, description STRING>,
        `Navigation` STRUCT<score STRING, description STRING>,
        `HTML5 Browser` STRUCT<score STRING, description STRING>,
        `PDF Renderer` STRUCT<score STRING, description STRING>,
        `Photo Library` STRUCT<score STRING, description STRING>,
        `Clang` STRUCT<score STRING, description STRING>,
        `Text Processing` STRUCT<score STRING, description STRING>,
        `Asset Compression` STRUCT<score STRING, description STRING>,
        `Object Detection` STRUCT<score STRING, description STRING>,
        `Background Blur` STRUCT<score STRING, description STRING>,
        `Horizon Detection` STRUCT<score STRING, description STRING>,
        `Object Remover` STRUCT<score STRING, description STRING>,
        `HDR` STRUCT<score STRING, description STRING>,
        `Photo Filter` STRUCT<score STRING, description STRING>,
        `Ray Tracer` STRUCT<score STRING, description STRING>,
        `Structure from Motion` STRUCT<score STRING, description STRING>
    >,
    multi_core_benchmarks STRUCT<
        `File Compression` STRUCT<score STRING, description STRING>,
        `Navigation` STRUCT<score STRING, description STRING>,
        `HTML5 Browser` STRUCT<score STRING, description STRING>,
        `PDF Renderer` STRUCT<score STRING, description STRING>,
        `Photo Library` STRUCT<score STRING, description STRING>,
        `Clang` STRUCT<score STRING, description STRING>,
        `Text Processing` STRUCT<score STRING, description STRING>,
        `Asset Compression` STRUCT<score STRING, description STRING>,
        `Object Detection` STRUCT<score STRING, description STRING>,
        `Background Blur` STRUCT<score STRING, description STRING>,
        `Horizon Detection` STRUCT<score STRING, description STRING>,
        `Object Remover` STRUCT<score STRING, description STRING>,
        `HDR` STRUCT<score STRING, description STRING>,
        `Photo Filter` STRUCT<score STRING, description STRING>,
        `Ray Tracer` STRUCT<score STRING, description STRING>,
        `Structure from Motion` STRUCT<score STRING, description STRING>
    >
);
```
"""

import ast
import json
from dataclasses import asdict

import pandas as pd
from google.cloud import bigquery
from prefect import flow, task

from utils.core.bigquery_helper import (
    get_cpu_model_id_and_result_id_for_scraping_details_df,
    load_df_to_bq,
)
from utils.core.geekbench.geekbench_processor_detail_scraper import GeekbenchProcessorDetailScraper
from utils.prefect_utility import generate_flow_name


def dumps_columns(geekbench_processor_detail_dict: dict) -> dict:
    # Convert the following fields to JSON string for geekbench_processor_detail_dict
    for field in [
        "system_info",
        "cpu_info",
        "memory_info",
        "single_core_benchmarks",
        "multi_core_benchmarks",
    ]:
        if field in geekbench_processor_detail_dict:
            geekbench_processor_detail_dict[field] = json.dumps(
                geekbench_processor_detail_dict[field],
                ensure_ascii=False,
            )

    return geekbench_processor_detail_dict

@task
def e_get_cpu_model_id_and_result_id_for_scraping_details_df() -> pd.DataFrame:
    df = get_cpu_model_id_and_result_id_for_scraping_details_df()
    print(df)
    return df

@task
def e_fetch_geekbench_processor_details(cpu_model_result_id_df: pd.DataFrame) -> list[dict]:
    geekbench_processor_detail_with_model_id_list = []
    for idx, row in cpu_model_result_id_df.iterrows():
        cpu_result_id = row["cpu_result_id"]
        cpu_model_id = row["cpu_model_id"]
        print(cpu_model_id, cpu_result_id, idx)
        scraper = GeekbenchProcessorDetailScraper(cpu_result_id)
        result = scraper.scrape_detail_page()
        geekbench_processor_detail_dict = asdict(result)

        # geekbench_processor_detail_dict = dumps_columns(geekbench_processor_detail_dict)

        geekbench_processor_detail_dict["cpu_model_id"] = cpu_model_id

        # print(geekbench_processor_detail_dict)

        geekbench_processor_detail_with_model_id_list.append(
            geekbench_processor_detail_dict,
        )

    return geekbench_processor_detail_with_model_id_list

@task
def t_prepare_geekbench_data(geekbench_processor_detail_with_model_id_list: list[dict]) -> list[dict]:
    processed_list = []

    numeric_columns = [
        "views",
        "single_core_score",
        "multi_core_score",
        "cpu_result_id",
        "cpu_model_id",
    ]

    record_columns = [
        "system_info",
        "cpu_info",
        "memory_info",
        "single_core_benchmarks",
        "multi_core_benchmarks",
    ]

    def parse_to_dict(x):
        if isinstance(x, dict):
            return x
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip()
            # Try JSON
            try:
                return json.loads(x)
            except json.JSONDecodeError:
                pass
            # Try Python literal (e.g. {'a': 'b'})
            try:
                val = ast.literal_eval(x)
                if isinstance(val, dict):
                    return val
            except (ValueError, SyntaxError) as e:
                print(f"WARNING: Failed to parse string: {x!r}")

        # Fallback
        return None

    for item in geekbench_processor_detail_with_model_id_list:
        row = item.copy()

        # 1. Cleaner Numerics
        for col in numeric_columns:
            if col in row and row[col] is not None:
                val = row[col]
                if isinstance(val, str):
                    val = val.replace(",", "").strip()
                    if not val:
                        row[col] = None
                        continue
                try:
                    row[col] = int(val)
                except (ValueError, TypeError):
                    row[col] = None

        # 2. Parse Records
        for col in record_columns:
            if col in row:
                row[col] = parse_to_dict(row[col])

        # 3. Format Date
        # BQ requires YYYY-MM-DD HH:MM:SS
        if "upload_date" in row and row["upload_date"]:
            val = row["upload_date"]
            if isinstance(val, str):
                try:
                    # Use pandas to handle various date formats (like "December 14 2025 04:30 AM")
                    dt = pd.to_datetime(val)
                    row["upload_date"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"WARNING: Parse date failed: {val} {e}")
                    row["upload_date"] = None

        processed_list.append(row)

    return processed_list

@task
def l_load_data_to_bq(data: list[dict]) -> None:
    if not data:
        print("No data to load.")
        return

    client = bigquery.Client()
    table_id = "geekbench_report.cpu_model_details" # Using dataset.table format

    # Configure job to append data
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # autodetect=True # Table exists, let BQ match schema
    )

    try:
        job = client.load_table_from_json(data, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.
        print(f"Loaded {len(data)} rows into {table_id}.")
    except Exception as e:
        print(f"Failed to load data to BigQuery: {e}")
        if hasattr(e, 'errors'):
            print(f"Errors: {e.errors}")
        raise e

@flow(name=generate_flow_name())
def sync_cpu_model_detail_to_bq() -> None:
    cpu_model_result_id_df = e_get_cpu_model_id_and_result_id_for_scraping_details_df()
    print("=====")

    geekbench_processor_detail_with_model_id_list = e_fetch_geekbench_processor_details(
        cpu_model_result_id_df,
    )

    processed_data = t_prepare_geekbench_data(
        geekbench_processor_detail_with_model_id_list,
    )

    l_load_data_to_bq(
        processed_data,
    )


if __name__ == "__main__":
    sync_cpu_model_detail_to_bq()
