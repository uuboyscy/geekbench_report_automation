from flows.sync_cpu_model_benchmark_to_bq_flow import sync_cpu_model_benchmarks_to_pg

if __name__ == "__main__":
    sync_cpu_model_benchmarks_to_pg.from_source(
        source="https://github.com/uuboyscy/geekbench_report_automation.git",
        entrypoint="src/flows/sync_cpu_model_benchmark_to_bq_flow.py:sync_cpu_model_benchmarks_to_pg",
    ).deploy(
        name="main",
        work_pool_name="process-pool",
        cron="0 7,11,15 * * *",
        tags=["geekbench-report"],
    )
