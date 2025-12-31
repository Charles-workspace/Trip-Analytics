# Trip Analytics : Snowpark ETL Pipeline with NYC Trip and Weather Data

Trip Analytics is a Snowflake-native ETL pipeline that ingests NYC taxi trip records and NOAA weather observations, performs data quality validation, executes transformations using Snowpark and publishes a curated dataset for downstream analytics. This project demonstrates DataOps, including Infrastructure-as-Code (IaC), automated DQ gates and decoupled orchestration.

This repository is part of my portfolio and demonstrates how I design and operationalize scalable data flows using cloud native tooling.


## System Architecture & Schema Design

The project implements a strict separation of concerns using functional namespaces within Snowflake. 

![Architecture](architecture.jpg)

### **1. Inbound Integration**s
* **Landing Area**: Raw ingestion from Snowflake internal stages.
    * `INBOUND_INTEGRATION.LANDING_TRIP.YELLOW_TRIP_RECORDS`
    * `INBOUND_INTEGRATION.LANDING_WEATHER.NYC_WEATHER`
* **Data Quality (DQ) Layer**: Dedicated tables for failed records.
    * `INBOUND_INTEGRATION.DQ_TRIP.TRIP_DATA_DQ`
    * `INBOUND_INTEGRATION.DQ_WEATHER.WEATHER_DATA_DQ`
* **Standardized Data Store (SDS)**: Validated, de-duplicated and pivoted records.
    * `INBOUND_INTEGRATION.SDS_TRIP.TRIP_DATA_VALIDATED`
    * `INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_VALIDATED`

### **2. Outbound Integration **
* **Analytics Layer**: The final curated dataset optimized for BI and ML.
    * `OUTBOUND_INTEGRATION.TRIP_ANALYTICS.TRIP_ANALYTICS`

## Tech Stack & Engineering Standards

* **Compute**: **Snowpark Python Stored Procedures** (Sprocs) execute logic directly within Snowflake warehouses, reducing data movement.
* **Orchestration**: **Apache Airflow** using **Datasets** to manage cross-DAG dependencies (Triggering the main transformation only when upstream ingestion is successful).
* **Infrastructure**: **Terraform** manages the full lifecycle of Snowflake objects (Databases, Schemas, Stages and Sprocs).
* **Observability**: Integrated **Snowflake Telemetry (`snowflake.telemetry.events`)** for structured logging and error tracing within Python handlers. 
* **CI/CD**: GitHub Actions utilizing **SnowCLI** for automated deployment and **PyLint** for static code analysis.

## Stored Procedures

The ETL workflow is executed using three Snowpark Python Stored Procedures:

| Stored Procedure | Responsibility |
|---|---|
| `OPS.PROC.SP_LOAD_TRIP_FROM_STAGE` | Loads trip files from internal stage into landing tables |
| `OPS.PROC.SP_LOAD_WEATHER_FROM_STAGE` | Loads NOAA weather CSV into landing tables |
| `OPS.PROC.SP_RUN_TRIP_PIPELINE` | Executes full pipeline: DQ validation → SDS load → transformation → publish to analytics layer |

These Stored Procedures act as the execution entrypoints for ingestion and transformation, and can be triggered manually, via CI/CD, or through orchestration.

## Data Pipeline Logic

### **Ingestion & Validation**
1.  **Stage-to-Landing**: 
  - Weather data is fetched from NCEI, validated via **Pydantic** during API retrieval and placed in Snowflake internal stage. 
  - Trip data is (tentatively) manually placed to a Snowflake stage.
  - Data is copied from Snowflake internal stages to dedicated landing tables.
2.  **DQ Gate**: The pipeline performs three critical checks:
    * **Null Check**: Identifies missing critical dimensions.
    * **Duplicate Check**: Ensures record uniqueness.
    * **Junk Value Check**: Filters out-of-range or logically impossible values.
3.  **Divergent Flow**: Records failing DQ are diverted to DQ tables with error descriptions wherever required, while clean records proceed to the **SDS** layer.

### **Transformation**
From SDS layer the trip data and weather observations are transformed and joined to obtain the final processed data as per the intended business logic. All table and schema names are managed via **Python DataClasses**, ensuring the code is fully parameterized and environment-agnostic.

## Testing & Quality Assurance

* **Unit Testing**: Python `unittest` suite validates transformation logic and data parsing.
* **Linting**: Strict `PyLint` enforcement in the CI/CD pipeline ensures PEP8 compliance and code quality.
* **Idempotency**: All ingestion and transformation logic is designed to be idempotent, supporting safe re-runs of the Airflow DAGs.

## Roadmap
* Integrate **AWS S3** and **Snowpipe** to enable automated, event-driven Trip data ingestion.
* Implement **Snowpark ML** to predict trip fare variations based on weather conditions.