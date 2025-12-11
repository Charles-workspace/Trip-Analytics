# Trip Analytics

This project builds and orchestrates an analytics pipeline for trip data using **Snowflake Python Stored Procedures**. It includes continuous integration and deployment (CI/CD) via **GitHub Actions** and leverages **SnowCLI** for secure authentication.

## Input Data

There are two public datasets used in this project:

- Newyork city Trip records data (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Weather data for Newyork city (https://openweathermap.org/api)

## Project Overview

The project aims to build a Snowpark-based ETL pipeline that combines NYC yellow taxi trip records with weather data to analyze how weather conditions impact taxi fare variations.

Trip data consists of parquet files for ride records and a CSV file for taxi zone lookups. These files are uploaded to a Snowflake stage using *SnowSQL*. Weather data is retrieved programmatically from NOAA’s Climate Data API for date range corresponding to trip records. All raw data is loaded into a landing schema before transformations begin. The final processed dataset is written to Snowflake tables.

## **API Integration**

The weather data is sourced through a minimal API integration layer that fetches daily weather observations directly from NOAA’s API for the required date range.  
The responses pass through Pydantic validation, basic error handling, and retry logic before being staged as CSV files for downstream processing inside Snowflake.


## Tools and Tech Stack

- Terraform - Infrastructure-as-code to provision all Snowflake objects
- Snowflake - Centralised cloud data platform used for staging, transformations and storage.
- Snowflake Stored Procedures (Python) – Executes the full ETL pipeline inside Snowflake.
- Github actions - Automates unit testing and deployment workflows.
- Docker - Containerization (used earlier; retained for local execution) 

## Data Transformation

1. The data is read from the landing schema as *Snowpark DataFrames*.
2. It undergoes three Data Quality (DQ) checks:
   - Null value check
   - Duplicate check
   - Junk value check
3. Records failing each check are logged into separate Snowflake tables for record purposes.
4. Only clean records proceed through transformation pipelines.
5. Transformed weather and trip data are joined and filtered to retain only relevant columns, then written to a single Snowflake output table.


## Roadmap

- Orchestrate Snowpark procedure with snowflake task
- Implement logging
- Package management using Poetry/ UV
- Lynting using PyLint
- To trigger integration testing upon merge against main branch
- Orchestrate with Snowpipe and Airflow
- The original intent of the project is to predict the Trip fare based on weather conditions with the help of machine learning models, which is now set as a target for the roadmap

## Notes

- The CI/CD pipeline currently:
  - Runs unit tests on Pull Requests (PRs) targeting `main`
  - Builds and deploys updated Python packages and stored procedures to Snowflake upon manual execution.
- All Snowflake credentials and configuration values are securely managed via GitHub secrets.