# Geekbench Report Automation

This project automates the collection and reporting of Geekbench CPU benchmark data. It utilizes Prefect for orchestration, BigQuery for data storage, and Google Sheets for reporting.

## Data Flow

The following diagram illustrates the data dependencies and flow between components:

```mermaid
graph TD
    subgraph Sources
        Web_Benchmarks[Web: Processor Benchmarks]
        Web_Names[Web: Processor Names]
        Web_Results[Web: Processor Results]
        Web_Details[Web: Processor Details]
    end

    subgraph BigQuery
        BQ_Benchmarks[geekbench_report.cpu_model_benchmarks]
        BQ_Names[geekbench_report.cpu_model_names]
        BQ_Results[geekbench_report.cpu_model_results]
        BQ_Details[geekbench_report.cpu_model_details]
        BQ_System[geekbench_report.system_names]
        BQ_Mart[Mart View: Score Report]
    end

    subgraph GoogleSheets
        GSheets[Google Sheets]
    end

    subgraph Flows
        Flow_Benchmarks[sync_cpu_model_benchmark_to_bq_flow]
        Flow_Names[sync_cpu_model_name_to_bq_flow]
        Flow_Results[sync_cpu_model_result_to_bq_flow]
        Flow_Details[sync_cpu_model_detail_to_bq_flow]
        Flow_Sheets[sync_bq_to_googlesheets_flow]
    end

    %% Benchmark Flow
    Web_Benchmarks --> Flow_Benchmarks
    Flow_Benchmarks --> BQ_Benchmarks

    %% Names Flow
    Web_Names --> Flow_Names
    Flow_Names --> BQ_Names

    %% Results Flow
    BQ_Names --> Flow_Results
    BQ_System -.-> Flow_Results
    Web_Results --> Flow_Results
    Flow_Results --> BQ_Results
    Flow_Results --> BQ_System
    Flow_Results --> BQ_Names

    %% Details Flow
    BQ_Results --> Flow_Details
    Web_Details --> Flow_Details
    Flow_Details --> BQ_Details

    %% Sheets Flow
    BQ_Results -.-> BQ_Mart
    BQ_Benchmarks -.-> BQ_Mart
    BQ_Details -.-> BQ_Mart
    BQ_Mart --> Flow_Sheets
    Flow_Sheets --> GSheets
```

## Flows

- **`sync_cpu_model_benchmark_to_bq_flow.py`**: Scrapes processor benchmarks from Geekbench Browser and saves to `cpu_model_benchmarks`.
- **`sync_cpu_model_name_to_bq_flow.py`**: Scrapes all available CPU model names and updates `cpu_model_names`.
- **`sync_cpu_model_result_to_bq_flow.py`**: Iterates through CPU models, scrapes their results pages (incremental update supported), and saves to `cpu_model_results`. dynamically updates `system_names` and `cpu_model_names` if new entities are found.
- **`sync_cpu_model_detail_to_bq_flow.py`**: Scrapes detailed specifications for CPU models found in results but missing details, saving to `cpu_model_details`.
- **`sync_bq_to_googlesheets_flow.py`**: Reads aggregated data from a BigQuery Mart view and updates a Google Sheet report.
