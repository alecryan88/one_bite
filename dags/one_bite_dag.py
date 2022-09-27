from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

#Custom Python modules
from modules.dbt_dag_parser import DbtDagParser
from modules.dbt_to_sheets_parser import DbtSheetsParser
from modules.one_bite_to_s3 import oneBiteToS3Operator

import logging
import os
from datetime import datetime, timedelta


#Global variables
SNOWFLAKE_CONN_ID = 'SNOWFLAKE_CONN'
SNOWFLAKE_RAW_TABLE = os.environ['SNOWFLAKE_RAW_TABLE']
SNOWFLAKE_STAGE = os.environ['SNOWFLAKE_STAGE']
SNOWFLAKE_RAW_SCHEMA = os.environ['SNOWFLAKE_RAW_SCHEMA']
SNOWFLAKE_ROLE = os.environ['SNOWFLAKE_ROLE']
S3_BUCKET = os.environ['S3_BUCKET']
DBT_PROFILES_DIR = os.environ['DBT_PROFILES_DIR']
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = "dev"
SHEET_NAME='one_bite'
DBT_SHEETS_TAG='sheets' #Models tagged w/ this are copied to google sheets

with DAG(
	'one_bite',
	default_args={
		#'retries': 1,
		#'retry_delay': timedelta(minutes=1)
	},
	description='A DAG for analyzing one bite reviews.',
	schedule_interval=timedelta(days=1),
	start_date=datetime(2021, 1, 1),
	catchup=False,
	tags=['one_bite'],
	template_searchpath='/opt/airflow/include/'
) as dag:

	def set_run_config(ds, **kwargs):
		#Checks to see if runtime config was passed via manual trigger
		manual_trigger_config = kwargs["dag_run"].conf 
		
		try:
			run_start = manual_trigger_config['backfill_start']
			run_end = manual_trigger_config['backfill_end']
			backfill_status = True
			logging.info(f'The backfill status is {backfill_status}')
		
		except KeyError:
			backfill_status = False
			run_start = ds
			run_end = ds
			logging.info(f'The backfill status is {backfill_status}')


		logging.info(f'The run_start and run_end are {run_start} and {run_end}')

		kwargs['ti'].xcom_push(key='run_start',value=run_start)
		kwargs['ti'].xcom_push(key='run_end',value=run_end)
		kwargs["ti"].xcom_push(key='backfill_status', value=backfill_status)


	set_run_config = PythonOperator(
		task_id='set_run_config',
		python_callable=set_run_config,
		provide_context=True
	)

	oneBiteToS3Operator = oneBiteToS3Operator(
		task_id='oneBiteToS3Operator',
		bucket = S3_BUCKET
	)

	create_table = SnowflakeOperator(
		task_id='create_table',
		snowflake_conn_id=SNOWFLAKE_CONN_ID,
		sql = 'create_table.sql',
		params={
			'table': SNOWFLAKE_RAW_TABLE,
			'schema': SNOWFLAKE_RAW_SCHEMA,
		}

	)

	delete_from_table = SnowflakeOperator(
		task_id='delete_from_table',
		snowflake_conn_id=SNOWFLAKE_CONN_ID,
		sql = 'delete_from_table.sql',
		params={
			'table': SNOWFLAKE_RAW_TABLE,
			'schema': SNOWFLAKE_RAW_SCHEMA,
		}

	)

	copy_into_table = SnowflakeOperator(
		task_id='copy_into_table',
		snowflake_conn_id=SNOWFLAKE_CONN_ID,
		sql = 'copy_into_table.sql',
		params={
			'table': SNOWFLAKE_RAW_TABLE,
			'schema': SNOWFLAKE_RAW_SCHEMA,
			'stage': SNOWFLAKE_STAGE
		}

	)

	dbt_compile = BashOperator(
		task_id='dbt_compile',
		bash_command=(
			f"dbt compile --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROFILES_DIR}"
		)
	)

	dag_parser = DbtDagParser(
		dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
		dbt_project_dir=DBT_PROFILES_DIR,
		dbt_profiles_dir=DBT_PROFILES_DIR,
		dbt_target=DBT_TARGET,
		run_start= "{{ ti.xcom_pull(key='run_start') }}",
		run_end= "{{ ti.xcom_pull(key='run_end') }}"
	)

	dbt_run_group = dag_parser.get_dbt_run_group()
	dbt_test_group = dag_parser.get_dbt_test_group()

	dbt_sheets_parser = DbtSheetsParser(
		conn_id=SNOWFLAKE_CONN_ID,
		sheet_name=SHEET_NAME,
		dbt_project_dir=DBT_PROFILES_DIR,
		dbt_sheets_tag=DBT_SHEETS_TAG

	)

	dbt_sheets_group = dbt_sheets_parser.get_dbt_sheets_group()
	

set_run_config >> oneBiteToS3Operator >> create_table >> delete_from_table >> copy_into_table >> dbt_compile >> dbt_run_group >> dbt_test_group >> dbt_sheets_group