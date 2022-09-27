# One Bite Pizza Review Data Pipeline

An end-to-end data application to analyze pizza reviews.


## Overview
https://onebite.app is a mobile application created by Barstool Sports, a leading sports and pop culture media company. The mobile application allows users to review pizza restaurants in their area and give a short description of their experience along with an overall score.

In this project, the review data is collected via the One Bite REST API and staged in Amazon S3. The data is then copied into Snowflake where it is transformed using dbt. The transformed models are copied to a Google Sheet where Tableau Public can connect and display the data for free. This entire ELT workflow is orchestrated by Apache Airflow running in Docker containers.

## Motivation
The reason I chose to do this project is to gain some experience using tools such as: Apache Airflow, DBT & Snowflake. I also wanted to showcase some SQL and Tableau skills that I've honed working as a Senior Business Intelligence Analyst.
