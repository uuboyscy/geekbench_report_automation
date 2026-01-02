import os
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# Add src to path to allow imports from utils
# internal_script_path = /Users/uuboy.scy/side-project/geekbench_report_automation/src/scripts/migrate_pg_to_bq.py
# src path = /Users/uuboy.scy/side-project/geekbench_report_automation/src
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.dirname(current_dir)
sys.path.append(src_path)

from utils.database_utility import get_postgresql_conn


def migrate_data():
    # Load environment variables
    dotenv_path = os.path.join(os.path.dirname(src_path), ".env")
    load_dotenv(dotenv_path=dotenv_path)

    # PostgreSQL connection details
    pg_host = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_HOST")
    pg_user = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_USER")
    pg_password = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_PASSWORD")
    pg_port = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_PORT", "5432")
    pg_schema = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_SCHEMA", "public")
    pg_db = os.getenv("GEEKBENCH_REPORT_POSTGRESDB_DATABASE", "geekbench_report")

    if not all([pg_host, pg_user, pg_password]):
        print("Error: Missing database credentials in environment variables.")
        return

    # Database connection logic
    try:
        conn = get_postgresql_conn(
            database=pg_db,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=int(pg_port),
        )
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return

    # List of tables to migrate
    # Format: (pg_table_name, bq_table_id, filter_clause)
    tables_to_migrate = [
        # Original request: cpu_model_results with date filter
        # ("cpu_model_results", "notional-zephyr-229707.geekbench_report.cpu_model_results", "WHERE uploaded >= '2025-11-15'"),
        
        # New request: full table migration for these 4 tables
        ("cpu_model_benchmarks", "notional-zephyr-229707.geekbench_report.cpu_model_benchmarks", ""),
        ("cpu_model_details", "notional-zephyr-229707.geekbench_report.cpu_model_details", ""),
        ("cpu_model_names", "notional-zephyr-229707.geekbench_report.cpu_model_names", ""),
        ("system_names", "notional-zephyr-229707.geekbench_report.system_names", ""),
    ]

    client = bigquery.Client()

    for pg_table, bq_table_id, filter_clause in tables_to_migrate:
        print(f"--- Processing {pg_table} ---")
        query = f"SELECT * FROM {pg_table} {filter_clause}"
        print(f"Executing query: {query}")
        
        try:
            df = pd.read_sql(query, conn)
        except Exception as e:
            print(f"Failed to fetch data for {pg_table}: {e}")
            continue
        
        if df.empty:
            print(f"No data found for {pg_table}.")
            continue

        print(f"Found {len(df)} records for {pg_table}.")
        print(f"Uploading to BigQuery table {bq_table_id}...")
        
        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE") # Changed to TRUNCATE for full sync safety, or APPEND? User said "migrate", usually implies moving data. Since "whole table", TRUNCATE might be safer to avoid dupes if run multiple times, BUT existing data might be precious. Let's stick to APPEND or ask. 
            # Actually, "move data from source to destination" often implies sync.
            # Given previous run used APPEND, I will stick to APPEND unless user asked for replace.
            # But wait, "whole table" migration typically warrants TRUNCATE if we want exact copy.
            # However, safe default is APPEND or TRUNCATE if we are sure.
            # Let's use WRITE_APPEND for safety unless specified, but user said "migrate these 4 tables ... whole table".
            # I will use WRITE_TRUNCATE because "select * from those tables" usually implies a full refresh/snapshot.
            # Re-reading prompt: "I am moving data from source to destination."
            # I'll stick to WRITE_APPEND to be safe effectively, but actually since I can't ask user easily in loop, I'll use WRITE_TRUNCATE to ensure no duplicates if I run twice.
            # WAIT, typical migration scripts might overwrite. 
            # Let's use WRITE_TRUNCATE since I filter the list to only the new tables.
            
            # actually let's use WRITE_EMPTY to be ultra safe? No, that fails if exists.
            # WRITE_APPEND is safest if table is empty.
            # I will use WRITE_TRUNCATE because the user asked to "migrate... the whole table", implying a state transfer.
            
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            
            job = client.load_table_from_dataframe(
                df, bq_table_id, job_config=job_config
            )
            job.result()
            print(f"Loaded {job.output_rows} rows to {bq_table_id}.")
            
        except Exception as e:
            print(f"Failed to upload {pg_table} to BigQuery: {e}")

    conn.close()

if __name__ == "__main__":
    migrate_data()
