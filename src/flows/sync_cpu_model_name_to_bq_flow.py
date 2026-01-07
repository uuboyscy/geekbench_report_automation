"""
Sync CPU model names to BigQuery.

SQL for creating table `cpu_model_names` in BigQuery:
```sql
CREATE TABLE `geekbench_report.cpu_model_names` (
    cpu_model_id INT64,
    cpu_model STRING
);
```
"""

from prefect import flow, task

from utils.core.bigquery_helper import update_cpu_model_names
from utils.core.geekbench.geekbench_processor_name_scraper import GeekbenchProcessorNameScraper
from utils.prefect_utility import generate_flow_name


@task
def e_fetch_all_cpu_model_names() -> list[str]:
    scraper = GeekbenchProcessorNameScraper()
    all_cpu_model_list = scraper.scrape_all_cpu_models()
    return all_cpu_model_list
  
@task
def l_update_cpu_model_names(check_update_list: list[str]) -> None:
    update_cpu_model_names(check_update_list)

@flow(name=generate_flow_name())
def sync_cpu_model_names_to_bq() -> None:
    """Sync CPU model names to BigQuery."""
    all_cpu_model_list = e_fetch_all_cpu_model_names()
    l_update_cpu_model_names(all_cpu_model_list)


if __name__ == "__main__":
    sync_cpu_model_names_to_bq()
