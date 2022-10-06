# One Bite Data Pipeline

An end-to-end data application to analyze pizza reviews.

## Overview
https://onebite.app is a mobile application created by Barstool Sports, a leading sports and pop culture media company. The mobile application allows users to review pizza restaurants in their area and give a short description of their experience along with an overall score. The app has a web version that exposes an API where review data can be downloaded.

## Motivation
The motivation for this project is primarily to demonstratee experience using Apache Airflow & DBT. A secondary goal of the project is to showcase SQL and Tableau proficiency. The final, and most important goal, is to find the best Pizza in my area.  


## Architecture
<img src="pipeline.png" width=100% height=70%>

## Airflow DAG
The Airflow DAG (Directed Acyclic Graph) below:
=======
<img src="https://github.com/alecryan88/one_bite/blob/main/pipeline.png" width=100% height=10%>

In this project, the user generated review data is collected via the One Bite REST API and staged in an Amazon S3 bucket. The data is then copied from the S3 bucket into Snowflake where it is transformed using dbt. The transformed models that have a specific tag in the dbt project are copied to a Google Sheet where my [Tableau Public Dashboard](https://public.tableau.com/app/profile/alec7813/viz/OneBiteMetrics/OneBiteMetrics) can connect and display the data for free. This entire pipeline is orchestrated by Apache Airflow running locally in Docker containers. 

- `set_run_config`: Tells the DAG run if it is backfilling data or performing an incremental load. This produces two dates run_start and run_end that are used in downstream tasks.

- `oneBiteToS3Operator`: A custom operator that extracts data for rewiews that occur between the run_start and run_end from the One Bite website by looping through the review webpages.

- `create_table`: Executes a SQL script in Snowflake that creates a table to copy from an S3 stage into if it doesn't already exist.

- `delete_from_table`: Executes a SQL script in Snowflake that deletes from the table previously created between the run_start and run_end.

- `copy_into_table`: Executes a SQL script to copy data from the S3 stage into Snowflake.

- `dbt_compile`: Executes a dbt compile command to generate a fresh manifest.json file.

- `dbt_run`: Parses the dbt manifest and creates an Airflow task group that runs all dbt models.

- `dbt_test`: Parses the dbt manifest and creates an Airflow task group that tests all dbt models.

- `dbt_to_sheets`: Parses the dbt manifest, finds all models tagged with "sheets" and copies the model outputs to a google sheet using the custom SnowflakeToSheetsOperator. This google sheet acts as the serving layer for Tableau Public.



<img src="dag.png" width=100% height=70%>
### Project Directory

    .
    ├── dags                    # Dag files and modules used in dag
    ├── include                 # Additional SQL files used in dag
    ├── one_bite_dbt            # Dbt project directory
    ├── plugins                 
    ├── .gitignore              
    ├── docker-compose.yaml     # Container configuration file
    ├── Dockerfile              # File that builds the docker image
    ├── pipeline.png            # Architecture diagram created with draw.io
    ├── requirements.txt        # File containing dependencies for project
    └── README.md
