import json
import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pygsheets

class SnowflakeToSheetsOperator(BaseOperator):
    """ A custom operator for copying dbt models to google sheets.

    Args:
        conn_id: The airflow conn_id for your data warehouse
        sheet_name: The name of the sheet where the dbt model outputs will be copied to
        dbt_project_dir: The directory containing the dbt_project.yml
        dbt_profiles_dir: The directory containing the profiles.yml
        dbt_target: The dbt target profile (e.g. dev, prod)
        dbt_tag: Limit dbt models to this tag if specified.
        task_group_name: task group name
    """

    @apply_defaults
    def __init__(
            self,
            conn_id,
            sheet_name,
            model_name,
            *args, 
            **kwargs,
            
        ):
        super().__init__(*args, **kwargs)
        self.hook = SnowflakeHook(snowflake_conn_id=conn_id)
        self.sheet_name = sheet_name
        self.model_name = model_name


    def connect_to_sheet(self, sheet_name):
        """ Creates a connection to a google sheet 

        Args:
            sheet_name: The name of the sheet where the Snowflake data will be loaded
       
        """

        gc = pygsheets.authorize(service_account_file='/opt/airflow/dags/modules/service_file.json')
        self.sh = gc.open(sheet_name) # Open the google sheet phthontest


    def create_or_replace_worksheet(self, worksheet_name):
        """ Creates a worksheet if not exists and clears the sheet
        
        Args:
            worksheet_name: The name of the worksheet being created

        """

        try:
            self.wks = self.sh.worksheet_by_title(worksheet_name)
            self.wks.clear('*')
        except pygsheets.exceptions.WorksheetNotFound:
            self.wks = self.sh.add_worksheet(worksheet_name) 
        
            
    def execute(self, context):
        """ The code to execute when the runner calls the operator """

        self.connect_to_sheet(self.sheet_name)
        self.create_or_replace_worksheet(self.model_name)
        result = self.hook.get_pandas_df(
            #Getlast 180 days from model
            f"""
                SELECT * 
                FROM {self.model_name} 
                WHERE partition_date >= dateadd('day', -180, current_date)
                """
            )
        self.wks.set_dataframe(result, (1,1), fit=True)
