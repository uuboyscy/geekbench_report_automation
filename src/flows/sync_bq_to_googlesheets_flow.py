"""

"""

from datetime import datetime, timedelta, timezone

import pandas as pd
from prefect import flow, task

from utils.core.bigquery_helper import get_score_report_from_df
from utils.googlesheets_utility import load_dataframe_to_google_sheets_worksheet
from utils.prefect_utility import generate_flow_name


@task
def get_update_time_df() -> pd.DataFrame:
    now = datetime.now(timezone(timedelta(hours=8)))
    return pd.DataFrame([{"Last Update": now}])

@task
def e_get_score_report_from_df() -> pd.DataFrame:
    return get_score_report_from_df()

@task
def t_convert_type_to_str(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("").astype(str)

@task
def t_rename_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "cpu_codename": "Generation",
            "cpu_model": "Processor name",
            "median_single_core_score": "Single core (Median)",
            "median_multi_core_score": "Multi core (Median)",
            "benchmark_single_core_score": "Single core (Ranking)",
            "benchmark_multi_core_score": "Multi core (Ranking)",
            "mean_single_core_score": "Single core (Mean)",
            "mean_multi_core_score": "Multi core (Mean)",
            "trimmed_mean_single_core_score": "Single core (Mean excl. max/min)",
            "trimmed_mean_multi_core_score": "Multi core (Mean excl. max/min)",
            "max_single_core_score": "Max for single core",
            "min_multi_core_score": "Min for Multi core",
            "stddev_single_core_score": "Std for Single core",
            "stddev_multi_core_score": "Std for Multi core",
            "max_uploaded": "The lastest upload",
            "min_uploaded": "The earliest upload",
            "data_count": "Data count",
        }
    )

@task
def l_dataframe_to_google_sheets_worksheet(df: pd.DataFrame, spreadsheet_url: str, worksheet_title: str, start_address: tuple, copy_head: bool) -> None:
    load_dataframe_to_google_sheets_worksheet(
        df=df,
        spreadsheet_url=spreadsheet_url,
        worksheet_title=worksheet_title,
        start_address=start_address,
        copy_head=copy_head,
    )


@flow(name=generate_flow_name())
def sync_pg_to_googlesheets() -> None:
    score_report_df = get_score_report_from_df()
    update_time_df = get_update_time_df()

    score_report_df = t_rename_column(score_report_df)
    score_report_df = t_convert_type_to_str(score_report_df)

    l_dataframe_to_google_sheets_worksheet(
        df=score_report_df,
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1z9YaGs9yyJadfDJIoXaODJjOqwwMsHkJaEsLQx3J3Zo",
        worksheet_title="Score (test)",
        start_address=(2, 1),
        copy_head=False,
    )

    l_dataframe_to_google_sheets_worksheet(
        df=update_time_df,
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1z9YaGs9yyJadfDJIoXaODJjOqwwMsHkJaEsLQx3J3Zo",
        worksheet_title="Data date (test)",
        start_address=(2, 1),
        copy_head=False,
    )


if __name__ == "__main__":
    sync_pg_to_googlesheets()
