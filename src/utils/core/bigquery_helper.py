
import os
from datetime import datetime
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from utils.bigquery_utility import load_dataframe_to_bigquery

load_dotenv()

# Dataset name, default to 'geekbench_report' if not set
GEEKBENCH_REPORT_BIGQUERY_DATASET = os.getenv("GEEKBENCH_REPORT_BIGQUERY_DATASET", "geekbench_report")

def get_bq_client() -> bigquery.Client:
    return bigquery.Client()

def load_df_to_bq(
    df: pd.DataFrame,
    table_name: str,
    if_exists: Literal["fail", "replace", "append"] = "fail",
) -> None:
    # Map if_exists to write_disposition
    write_disposition_map = {
        "fail": "WRITE_EMPTY",
        "replace": "WRITE_TRUNCATE",
        "append": "WRITE_APPEND",
    }
    write_disposition = write_disposition_map.get(if_exists, "WRITE_EMPTY")
    
    destination = f"{GEEKBENCH_REPORT_BIGQUERY_DATASET}.{table_name}"
    
    client = get_bq_client()
    load_dataframe_to_bigquery(
        bigquery_client=client,
        dataframe=df,
        destination=destination,
        write_disposition=write_disposition,
    )

def get_cpu_model_name_list_from_bq() -> list[str]:
    client = get_bq_client()
    query = f"SELECT cpu_model FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names`"
    try:
        return client.query(query).to_dataframe()["cpu_model"].to_list()
    except Exception:
        # Return empty list if table logic fails or table doesn't exist
        return []

def get_system_name_list_from_bq() -> list[str]:
    client = get_bq_client()
    query = f"SELECT system FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.system_names`"
    try:
        return client.query(query).to_dataframe()["system"].to_list()
    except Exception:
        return []

def get_cpu_model_map_from_bq() -> dict[str, int]:
    """
    Return a dict with key as cpu_model and value as cpu_model_id.
    """
    client = get_bq_client()
    query = f"SELECT cpu_model, cpu_model_id FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names`"
    try:
        df = client.query(query).to_dataframe()
        return dict(zip(df["cpu_model"], df["cpu_model_id"]))
    except Exception:
        return {}

def get_system_map_from_bq() -> dict[str, int]:
    """
    Return a dict with key as system and value as system_id.
    """
    client = get_bq_client()
    query = f"SELECT system, system_id FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.system_names`"
    try:
        df = client.query(query).to_dataframe()
        return dict(zip(df["system"], df["system_id"]))
    except Exception:
        return {}

def update_cpu_model_names(check_update_list: list[str]) -> None:
    """Sync CPU model names to BigQuery."""
    df = pd.DataFrame(check_update_list, columns=["cpu_model"])

    # Read existing CPU model names
    existing_model_list = get_cpu_model_name_list_from_bq()
    existing_model_set = set(existing_model_list)

    # Find new CPU models that need to be added
    new_models = set(df["cpu_model"]) - existing_model_set
    if new_models:
        # We need to assign new IDs. In BQ, auto-increment isn't standard.
        # This function in PG assumes logic elsewhere handles ID or implicit serial? 
        # Wait, the PG code just loads `new_df` with `cpu_model` column. 
        # PG table `cpu_model_names` likely has `cpu_model_id SERIAL` or similar.
        # In BQ, we must generate IDs manually if we want them, or let them be null if schema allows.
        # BUT `get_cpu_model_map_from_pg` relies on `cpu_model_id`.
        # Assuming the user handles ID generation or we need to simple MAX(id) + 1 logic here?
        # The PG `load_df_to_pg` call just appends names. The DB probably handles ID.
        # For BQ, we should probably check if max ID is needed.
        # For now, I will mirror the PG logic: just insert names. 
        # WARNING: BQ doesn't auto-generate IDs. 
        # I'll modify logic to fetch max ID and increment if possible, or just insert.
        # Given "implement all function... with BigQuery version", I should probably handle ID generation if BQ requires it.
        # Let's check PG schema in other files if possible. 
        # The migration script showed:
        # { "name": "cpu_model_id", "type": "INTEGER" ... }
        # So IDs exist.
        # Be careful: Concurrent runs could duplicate IDs. For a simple script, MAX+1 is okay.
        
        client = get_bq_client()
        
        # Get max ID
        max_id_query = f"SELECT MAX(cpu_model_id) as max_id FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names`"
        try:
            res = client.query(max_id_query).to_dataframe()
            current_max = res["max_id"].iloc[0]
            if pd.isna(current_max):
                current_max = 0
        except:
            current_max = 0
            
        new_df = pd.DataFrame(list(new_models), columns=["cpu_model"])
        # Assign IDs
        new_df["cpu_model_id"] = range(current_max + 1, current_max + 1 + len(new_df))
        
        load_df_to_bq(
            df=new_df,
            table_name="cpu_model_names",
            if_exists="append",
        )
        print(f"Added {len(new_models)} new CPU models to BigQuery")
        # print(new_models)
    else:
        print("No new CPU models to add")

def update_system_names(check_update_list: list[str]) -> None:
    """Sync system names to BigQuery."""
    df = pd.DataFrame(check_update_list, columns=["system"])

    existing_system_list = get_system_name_list_from_bq()
    existing_system_set = set(existing_system_list)

    new_systems = set(df["system"]) - existing_system_set
    if new_systems:
        client = get_bq_client()
        # Get max ID
        max_id_query = f"SELECT MAX(system_id) as max_id FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.system_names`"
        try:
            res = client.query(max_id_query).to_dataframe()
            current_max = res["max_id"].iloc[0]
            if pd.isna(current_max):
                current_max = 0
        except:
            current_max = 0
            
        new_df = pd.DataFrame(list(new_systems), columns=["system"])
        new_df["system_id"] = range(current_max + 1, current_max + 1 + len(new_df))
        
        load_df_to_bq(
            df=new_df,
            table_name="system_names",
            if_exists="append",
        )
        print(f"Added {len(new_systems)} new systems to BigQuery")
        # print(new_systems)
    else:
        print("No new systems to add")

def get_last_updated_dates_of_cpu_model_df() -> pd.DataFrame:
    query = f"""
        with last_uploaded_record as (
            select
                cpu_model_id
                , max(uploaded) as last_uploaded
            from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_results`
            group by cpu_model_id
        )
        select
            d.cpu_model
            , COALESCE(f.last_uploaded, DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 30 DAY)) AS last_uploaded
        from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names` d
        left join last_uploaded_record f
        on d.cpu_model_id = f.cpu_model_id
        where d.cpu_model <> 'ARM'
        order by d.cpu_model_id
    """
    client = get_bq_client()
    return client.query(query).to_dataframe()

def get_cpu_model_id_and_result_id_for_scraping_details_df() -> pd.DataFrame:
    query = f"""
        with cpu_model_id_with_result_id as (
            select
                cpu_model_id,
                max(cpu_result_id) as cpu_result_id
            from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_results`
            group by cpu_model_id
        )
        select
            cpu_model_id,
            cpu_result_id
        from cpu_model_id_with_result_id
        where cpu_model_id not in (
            select cpu_model_id from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_details`
        )
    """
    client = get_bq_client()
    try:
        return client.query(query).to_dataframe()
    except Exception:
        # Return empty DF if failing (e.g. table missing)
        return pd.DataFrame()

def delete_cpu_model_result_record_from_date_to_now(
    cpu_model: str,
    from_date: str | datetime,
) -> None:
    client = get_bq_client()
    # BQ uses strings for dates in loose queries usually, but safe to cast if needed.
    # from_date is Python Object, f-string injection needs care.
    if isinstance(from_date, (datetime, pd.Timestamp)):
        from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        from_date_str = str(from_date)
        
    delete_sql = f"""
        DELETE FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_results`
        WHERE cpu_model_id = (
            SELECT cpu_model_id FROM `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names` WHERE cpu_model = @cpu_model
        )
        AND uploaded >= @from_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cpu_model", "STRING", cpu_model),
            bigquery.ScalarQueryParameter("from_date", "DATETIME", from_date_str), # Assuming uploaded is DATETIME
        ]
    )
    
    print(f"Deleting cpu_model='{cpu_model}'/uploaded>='{from_date_str}' from cpu_model_results...")
    job = client.query(delete_sql, job_config=job_config)
    job.result()
    print(f"{job.num_dml_affected_rows} rows affected.")

def delete_duplicated_cpu_model_result_from_bq() -> None:
    # Use Create or Replace Logic as DELETE dedup is hard
    table_id = f"{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_results"
    
    dedup_sql = f"""
        CREATE OR REPLACE TABLE `{table_id}` AS
        SELECT * EXCEPT(rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    partition by cpu_result_id, system_id, cpu_model_id, frequency, cores,
                                    uploaded, platform, single_core_score, multi_core_score
                ) as rn
            FROM `{table_id}`
        )
        WHERE rn = 1
    """
    client = get_bq_client()
    print("Deleting duplicated data from cpu_model_results (Redefining table)...")
    job = client.query(dedup_sql)
    job.result()
    print("Done.")
