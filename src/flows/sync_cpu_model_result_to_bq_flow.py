"""
Sync CPU model results to BigQuery.

SQL for creating table `cpu_model_results` in BigQuery:
```sql
CREATE TABLE `geekbench_report.cpu_model_results` (
    cpu_result_id INT64,
    frequency STRING,
    cores INT64,
    uploaded DATETIME,
    platform STRING,
    single_core_score INT64,
    multi_core_score INT64,
    cpu_model_id INT64,
    system_id INT64
);
```
"""

import os

import pandas as pd
from prefect import flow

from utils.core.bigquery_helper import (
    delete_duplicated_cpu_model_result_from_bq,
    get_cpu_model_map_from_bq,
    get_last_updated_dates_of_cpu_model_df,
    get_system_map_from_bq,
    load_df_to_bq,
    update_cpu_model_names,
    update_system_names,
)
from utils.core.geekbench.geekbench_processor_result_scraper import GeekbenchProcessorResultScraper
from utils.prefect_utility import generate_flow_name

OFFSET_FILE_PATH = "/tmp/sync_cpu_model_result_offset.txt"


def write_offset(offset_idx: int) -> None:
    """
    Write the current offset index to a local file.
    """
    with open(OFFSET_FILE_PATH, "w") as f:
        f.write(str(offset_idx))


def get_offset() -> int:
    """
    Read the offset index from the local file. If the file does not exist, return 0.
    """
    if not os.path.exists(OFFSET_FILE_PATH):
        return 0
    with open(OFFSET_FILE_PATH, "r") as f:
        try:
            return int(f.read().strip())
        except Exception:
            return 0


def delete_offset_file() -> None:
    """
    Delete the offset file if it exists.
    """
    if os.path.exists(OFFSET_FILE_PATH):
        os.remove(OFFSET_FILE_PATH)


@flow(name=generate_flow_name(), log_prints=True)
def sync_cpu_model_result_to_bq() -> None:

    offset_idx = get_offset()

    last_updated_dates_of_cpu_model_df = get_last_updated_dates_of_cpu_model_df()
    system_map = get_system_map_from_bq()
    cpu_model_map = get_cpu_model_map_from_bq()

    all_df_list = []
    for idx, row in last_updated_dates_of_cpu_model_df.loc[offset_idx:].iterrows():
        cpu_model_name = row["cpu_model"]
        last_updated_date = row["last_uploaded"]

        # print(f"[{idx}] Processing {cpu_model_name}, from {last_updated_date}")
        with open("/tmp/sync_cpu_model_result_to_bq.log", "w") as f:
            f.write(f"[{idx}] Processing {cpu_model_name}, from {last_updated_date}")

        scraper = GeekbenchProcessorResultScraper(
            cpu_model_name,
            offset_date=last_updated_date,
        )

        df = scraper.scrape_multiple_pages_until_offset_date()
        if len(df) == 0:
            continue

        # update system_names and cpu_model_names if new one detected
        if df[~(df["system"].isin(system_map))].shape[0] > 0:
            update_system_names(df["system"].to_list())
            system_map = get_system_map_from_bq()
        if df[~(df["cpu_model"].isin(cpu_model_map))].shape[0] > 0:
            update_cpu_model_names(df["cpu_model"].to_list())
            cpu_model_map = get_cpu_model_map_from_bq()

        # system -> system_id , cpu_model -> cpu_model_id
        df["system_id"] = df["system"].map(system_map)
        df["cpu_model_id"] = df["cpu_model"].map(cpu_model_map)

        df_required_columns = df.drop(["system", "cpu_model"], axis=1)

        all_df_list.append(df_required_columns)

        # Flush
        if (idx + 1) % 250 == 0:
            print(pd.concat(all_df_list).drop_duplicates())
            load_df_to_bq(
                df=pd.concat(all_df_list).drop_duplicates(),
                table_name="cpu_model_results",
                if_exists="append",
            )
            delete_duplicated_cpu_model_result_from_bq()
            all_df_list = []
            write_offset(idx)

    # Final flush
    if all_df_list:
        load_df_to_bq(
            df=pd.concat(all_df_list).drop_duplicates(),
            table_name="cpu_model_results",
            if_exists="append",
        )
        delete_duplicated_cpu_model_result_from_bq()

    delete_offset_file()


if __name__ == "__main__":
    sync_cpu_model_result_to_bq()
