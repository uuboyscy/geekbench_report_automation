import os

from dotenv import load_dotenv

load_dotenv()

GEEKBENCH_REPORT_BIGQUERY_DATASET = os.getenv(
    "GEEKBENCH_REPORT_BIGQUERY_DATASET", "geekbench_report"
)

sql = f"""-- Base dataset: only the relevant columns
with base as (
	select
		cpu_model_id,
		single_core_score,
		multi_core_score,
		uploaded
	from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_results`
),

-- Main stats: mean, stddev, median, min/max, and counts
with_stats as (
	select
		cpu_model_id,
		AVG(single_core_score) as mean_single_core_score,
		STDDEV_POP(single_core_score) as stddev_single_core_score,
		APPROX_QUANTILES(single_core_score, 100)[OFFSET(50)] as median_single_core_score,
		AVG(multi_core_score) as mean_multi_core_score,
		STDDEV_POP(multi_core_score) as stddev_multi_core_score,
		APPROX_QUANTILES(multi_core_score, 100)[OFFSET(50)] as median_multi_core_score,
		MAX(single_core_score) as max_single_core_score,
		MIN(multi_core_score) as min_multi_core_score,
		MAX(uploaded) as max_uploaded,
		MIN(uploaded) as min_uploaded,
		COUNT(*) as data_count
	from base
	group by cpu_model_id
),

-- Trimmed mean: exclude first and last rows after sorting for each score
trimmed as (
	select
		cpu_model_id,
		AVG(single_core_score) as trimmed_mean_single_core_score,
		AVG(multi_core_score) as trimmed_mean_multi_core_score
	from (
		select
			cpu_model_id,
			single_core_score,
			multi_core_score,
			row_number() over (partition by cpu_model_id order by single_core_score) as rn_single,
			row_number() over (partition by cpu_model_id order by multi_core_score) as rn_multi,
			count(*) over (partition by cpu_model_id) as cnt
		from base
	) ranked
	where
		-- remove first and last records for trimmed mean
		rn_single > 1 and rn_single < cnt
		and rn_multi > 1 and rn_multi < cnt
	group by cpu_model_id
),

cpu_codename_dim as (
	select
		cpu_model_id,
		max(cpu_codename) as cpu_codename
	from `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_details`
	group by cpu_model_id
),

-- Final output: combine stats and model name
final_table as (
	select
		dim.cpu_model,
		detail.cpu_codename,
		ROUND(s.mean_single_core_score) as mean_single_core_score,
		ROUND(s.stddev_single_core_score) as stddev_single_core_score,
		ROUND(s.median_single_core_score) as median_single_core_score,
		ROUND(t.trimmed_mean_single_core_score) as trimmed_mean_single_core_score,
		ROUND(b.single_core_score) as benchmark_single_core_score,
		ROUND(s.mean_multi_core_score) as mean_multi_core_score,
		ROUND(s.stddev_multi_core_score) as stddev_multi_core_score,
		ROUND(s.median_multi_core_score) as median_multi_core_score,
		ROUND(t.trimmed_mean_multi_core_score) as trimmed_mean_multi_core_score,
		ROUND(b.multi_core_score) as benchmark_multi_core_score,
		ROUND(s.max_single_core_score) as max_single_core_score,
		ROUND(s.min_multi_core_score) as min_multi_core_score,
		s.max_uploaded,
		s.min_uploaded,
		s.data_count
	from with_stats s
	left join trimmed t
		on s.cpu_model_id = t.cpu_model_id
	left join `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_names` dim
		on s.cpu_model_id = dim.cpu_model_id
	left join `{GEEKBENCH_REPORT_BIGQUERY_DATASET}.cpu_model_benchmarks` b
		on dim.cpu_model = b.cpu_model
	left join cpu_codename_dim detail
		on s.cpu_model_id = detail.cpu_model_id
-- 	order by dim.cpu_model
)

select
	cpu_codename,
	cpu_model,
	median_single_core_score,
	median_multi_core_score,
	benchmark_single_core_score,
	benchmark_multi_core_score,
	mean_single_core_score,
	mean_multi_core_score,
	trimmed_mean_single_core_score,
	trimmed_mean_multi_core_score,
	max_single_core_score,
	min_multi_core_score,
	stddev_single_core_score,
	stddev_multi_core_score,
	max_uploaded,
	min_uploaded,
	data_count
from final_table
order by cpu_codename desc
"""

if __name__ == "__main__":
    print(sql)