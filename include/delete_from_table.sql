DELETE FROM {{ params.schema }}.{{ params.table }} 
WHERE partition_date between '{{ ti.xcom_pull(task_ids='set_run_config', key='run_start') }}' and '{{ ti.xcom_pull(task_ids='set_run_config', key='run_end') }}';