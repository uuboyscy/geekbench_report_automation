from flows.test_flow import test_flow

if __name__ == "__main__":
    test_flow.from_source(
        source="https://github.com/uuboyscy/geekbench_report_automation.git",
        entrypoint="src/flows/test_flow.py:test_flow",
    ).deploy(
        name="test",
        work_pool_name="process-pool",
        # image="my-image",
        # push=False,
        cron="0 * * * *",
        tags=["test"],
    )
