# Trip Analytics

This project builds and deploys a Docker-based analytics service for trip data using **Snowflake Snowpark Container Services (SPCS)**. It includes continuous integration and deployment (CI/CD) via **GitHub Actions** and leverages **SnowCLI** for secure authentication and image registry management.

## Input Data

There are two public datasets used in this project:

- Newyork city Trip records data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Weather data for Newyork city (https://openweathermap.org/api)

## Project Overview

The project aims to build a Snowpark-based ETL pipeline that combines NYC yellow taxi trip records with weather data to analyze how weather conditions impact taxi fare variations.

Trip data consists of parquet files for ride records and a CSV file for taxi zone lookups. These files are uploaded to a Snowflake stage using *SnowSQL*. Weather data is retrieved through OpenWeatherMap APIs for date range corresponding to trip records. All raw data is loaded into a landing schema before transformations begin. The final processed dataset is written to Snowflake tables.

## Tools and Tech Stack

- Terraform - Infrastructure-as-code to provision all Snowflake objects
- Snowflake - Centralised cloud data platform used for staging, transformations and storage.
- Docker - Containerization of the application for deployment.
- Snowpark container service (SPCS) - Hosts the Docker image for deployment.
- Github actions - Automates unit testing and deployment workflows.

## Data Transformation

1. The data is read from the landing schema as *Snowpark DataFrames*.
2. It undergoes three Data Quality (DQ) checks:
   - Null value check
   - Duplicate check
   - Junk value check
3. Records failing each check are logged into separate Snowflake tables for record purposes.
4. Only clean records proceed through transformation pipelines.
5. Transformed weather and trip data are joined and filtered to retain only relevant columns, then written to a single Snowflake output table.


## Current Limitation

- The project was implemented using snowflake free-tier in which compute pool is not included. So it was not possible to execute the deployed docker image of the project. Though the project is executable in local

## Roadmap

- Register as Snowpark procedure and orchestrate with snowflake task
- Implement logging
- Package management using Poetry/ UV
- Lynting using PyLint
- To trigger integration testing upon merge against main branch
- Orchestrate with Snowpipe and Airflow
- The original intent of the project is to predict the Trip fare based on weather conditions with the help of machine learning models, which is now set as a target for the roadmap

## Notes

- The CI/CD pipeline currently:
  - Runs unit tests on Pull Requests (PRs) targeting `main`
  - Triggers deployment to SPCS on merges to `main`
- All Snowflake credentials and configuration values are securely managed via GitHub secrets.