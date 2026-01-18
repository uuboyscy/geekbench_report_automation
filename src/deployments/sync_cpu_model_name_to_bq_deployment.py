from flows.sync_cpu_model_name_to_bq_flow import sync_cpu_model_names_to_bq

if __name__ == "__main__":
    sync_cpu_model_names_to_bq.from_source(
        source="https://github.com/uuboyscy/geekbench_report_automation.git",
        entrypoint="src/flows/sync_cpu_model_name_to_bq_flow.py:sync_cpu_model_names_to_bq",
    ).deploy(
        name="main",
        work_pool_name="process-pool",
        cron="0 6,10,14 * * *",
    )
