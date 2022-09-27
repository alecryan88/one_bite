from modules.snowflake_to_sheets import SnowflakeToSheetsOperator
import json
import logging
import os
import subprocess
from airflow.utils.task_group import TaskGroup


class DbtSheetsParser:
    """ A utility class that parses out a dbt project and copies tagged models to sheets.

    Args:
        dag: The Airflow DAG
        dbt_global_cli_flags: Any global flags for the dbt CLI
        dbt_project_dir: The directory containing the dbt_project.yml
        dbt_profiles_dir: The directory containing the profiles.yml
        dbt_target: The dbt target profile (e.g. dev, prod)
        dbt_tag: Limit dbt models to this tag if specified.
        task_group_name: task group name
    """

    def __init__(
        self,
        dag=None,
        conn_id=None,
        sheet_name=None,
        dbt_project_dir=None,
        dbt_target=None,
        dbt_sheets_tag=None,
        task_group_name='dbt_to_sheets'

    ):

        self.dag = dag
        self.conn_id = conn_id
        self.dbt_project_dir = dbt_project_dir
        self.dbt_target = dbt_target
        self.dbt_sheets_tag = dbt_sheets_tag
        self.sheet_name = sheet_name

        self.dbt_sheets_group = TaskGroup(task_group_name)
    
        # Parse the manifest and populate the task group
        self.make_dbt_sheets_task_group()


    def load_dbt_manifest(self):
        """ Helper function to load the dbt manifest file.

        Returns: 
            A JSON object containing the dbt manifest content.

        """
        manifest_path = os.path.join(self.dbt_project_dir, "target/manifest.json")
        with open(manifest_path) as f:
            file_content = json.load(f)
        return file_content


    def make_sheets_task(self, node_name):
        """ Creates a SnowflakeToSheetsOperator task to copy a dbt model output to sheets.
        
        Args:
            node_name: The name of the node
           
        Returns:    
            A SnowflakeToSheetsOperator task that copies the respective dbt model ouput to sheets

        """

        sheets_task = SnowflakeToSheetsOperator(
			task_id=node_name,
            task_group = self.dbt_sheets_group,
			conn_id=self.conn_id,
			sheet_name=self.sheet_name,
			model_name=node_name
        ),
        dag=self.dag
        
        logging.info(f"Copied {node_name} to google sheets.")
        
        return sheets_task


    def make_dbt_sheets_task_group(self):
        """ Parses dbt manifest and creates SnowflakeToSheetsOperator task for each tagged model """
        
        manifest_json = self.load_dbt_manifest()
        
        dbt_sheets_tasks = {}

        for node_name in manifest_json["nodes"].keys():
            if node_name.split(".")[0] == "model":
                for tag in manifest_json['nodes'][node_name]['tags']:
                    if self.dbt_sheets_tag in tag:
                        model = (node_name.split(".")[2])
                        dbt_sheets_tasks[model] = self.make_sheets_task(model)
                        

    def get_dbt_sheets_group(self):
        """ Retrieves the previously constructed SnowflakeToSheetsOperator tasks.
     
        Returns:
            dbt_sheets_group: An Airflow task group with dbt run nodes.
        """

        logging.info(self.dbt_sheets_group)
        return self.dbt_sheets_group