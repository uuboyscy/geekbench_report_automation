"""

"""

from datetime import datetime, timedelta, timezone

import pandas as pd

from utils.core.bigquery_helper import get_score_report_from_df
from utils.googlesheets_utility import load_dataframe_to_google_sheets_worksheet


def get_update_time_df() -> pd.DataFrame:
    now = datetime.now(timezone(timedelta(hours=8)))
    return pd.DataFrame([{"Last Update": now}])


def t_convert_type_to_str(df: pd.DataFrame) -> pd.DataFrame:
    return df.fillna("").astype(str)

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


def sync_pg_to_googlesheets() -> None:
    score_report_df = get_score_report_from_df()
    update_time_df = get_update_time_df()

    score_report_df = t_rename_column(score_report_df)
    score_report_df = t_convert_type_to_str(score_report_df)

    load_dataframe_to_google_sheets_worksheet(
        df=score_report_df,
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1z9YaGs9yyJadfDJIoXaODJjOqwwMsHkJaEsLQx3J3Zo",
        worksheet_title="Score (test)",
        start_address=(2, 1),
        copy_head=False,
    )

    load_dataframe_to_google_sheets_worksheet(
        df=update_time_df,
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1z9YaGs9yyJadfDJIoXaODJjOqwwMsHkJaEsLQx3J3Zo",
        worksheet_title="Data date (test)",
        start_address=(2, 1),
        copy_head=False,
    )


if __name__ == "__main__":
    sync_pg_to_googlesheets()
