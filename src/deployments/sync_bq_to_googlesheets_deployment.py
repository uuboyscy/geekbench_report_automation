from prefect.schedules import Cron

from flows.sync_bq_to_googlesheets_flow import sync_pg_to_googlesheets

if __name__ == "__main__":
    sync_pg_to_googlesheets.from_source(
        source="https://github.com/uuboyscy/geekbench_report_automation.git",
        entrypoint="src/flows/sync_bq_to_googlesheets_flow.py:sync_pg_to_googlesheets",
    ).deploy(
        name="main",
        work_pool_name="process-pool",
        schedules=[Cron("0 8,12,16 * * *", timezone="Asia/Taipei")],
        tags=["geekbench-report"],
    )
