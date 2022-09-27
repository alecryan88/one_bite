# One Bite Pizza Review Data Pipeline

An end-to-end data application to analyze pizza reviews.


## Overview
https://onebite.app is a mobile application created by Barstool Sports, a leading sports and pop culture media company. The mobile application allows users to review pizza restaurants in their area and give a short description of their experience along with a score (0-10). The review data is collected via the One Bite App REST API and stored in Amazon S3. Data is copied into Snowflake where it is transformed using DBT. The transformed models are copied to a Google Sheets where Tableau Public can connect and display the data for free.

## Motivation
The reason I chose to do this project is to gain some experience using tools such as: Apache Airflow, DBT & Snowflake. I also wanted to showcase some SQL and Tableau skills that I've honed working as a Senior Business Intelligence Analyst.
