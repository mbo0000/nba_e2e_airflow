# nba-sport-airflow
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green)
![Snowflake](https://img.shields.io/badge/Snowflake-%23f3f1ff)
![Docker](https://img.shields.io/badge/Docker-%2B-blue)

## Introduction

This repo contains the installation and setup of Airflow tool for the [NBA Stat project](https://github.com/mbo0000/nba_e2e_data_pipeline). The primary objective of this project is to automate the Extract and Load parts of the data pipeline.

## Table of Contents
- [DAG Overview](#dag-overview)
- [Installation and Setup](#installation-and-setup)
- [DAG Walkthrough](#dag-walkthrough)
- [Future Work and Improvement](#future-work-and-improvement)

## DAG Overview
In this project, we will be working with multiple API endpoints. Each endpoint will have a dedicated DAG with the follow tasks template:

- **Extract & Load**: Extracts NBA data via a dockerized Python container. New raw data will be landing in Snowflake staging area, ready for process.
- **Update Target Table Schema**: Checks and updates the target table schema if new columns are added to the source schema.
- **Merge**: Merge new data from the source to the staging area into the raw layer in Snowflake.
- **Cleanup**: remove the staging data.
- **Trigger Downstream DAGs**: Optionally, triggers additional DAG that has cross DAG dependency.

## Installation and Setup

### Prerequisites

- Python 3.10+
- Snowflake Account
- Apache Airflow (2.x)
- Docker

### Setup Steps
In your project folder:
1. Clone the NBA data extraction [repo](https://github.com/mbo0000/nba_e2e_extractor_package) and follow installation instruction.
2. Clone the airflow repository in the main project directory:
    ```sh
    git clone https://github.com/mbo0000/nba_e2e_airflow.git
    cd nba_e2e_airflow
3. In terminal, create a default network using the command below.
    ```sh
    docker network create airflow_default
    ```
    This will ensure the container is using the same local network. Airflow DAG will need to be able communicate with the NBA data extractor container.
4. Build image and spin up container:
    ```sh
    docker build -t airflow-nba-image:latest . && docker compose up -d
    ```
5. Once Airflow web UI is up and running, go to [http:localhost:8080](http:localhost:8080). Username and password are both `airflow`.
6. Create a Snowflake connection in the web UI and provide your Snowflake credentials in `Admin` >> `Connection` setting:
    - Connection Id: snowflake_conn
    - Connection Type: Snowflake
    - Login: airflow user created [here](https://github.com/mbo0000/nba_e2e_data_pipeline?tab=readme-ov-file#1-snowflake-management-and-config)
    - Password: airflow password created [here](https://github.com/mbo0000/nba_e2e_data_pipeline?tab=readme-ov-file#1-snowflake-management-and-config)
    - Account: XXXXX
    - Role: [pc_user](https://github.com/mbo0000/nba_e2e_data_pipeline?tab=readme-ov-file#1-snowflake-management-and-config) or replace with your selected role


## DAG Walkthrough
Each DAG is generated using the DAG generator, with configurations defined in `ENTITIES`. Depending on the configuration, the internal behaviors of each DAG will differ.

For each entity, it has following attributes:
- id: join key when merging new data into existing table
- case_field: key's used to update a record if exist based on condition
- trigger_dag_id: downstream DAG dependency
- schedule: DAG schedule

Each DAG will have the following tasks in order: 
1. Extract and load entity data to Snowflake via bash command entity arguments:
    ```
    extract_load = BashOperator(
        task_id         = 'extract_load'
        , bash_command  = f'docker exec nba-sport-extractor-app-1 python main.py '\
                            f' --entity {entity}'\
                            f' --database {SOURCE_DATABASE}'\
                            f' --schema {SOURCE_SCHEMA}'
        )
    ```
    Extract and load component is a dockerized python container locally hosted.

2. Once data landed in the staging area, update target table schema if there are new schema changes. If target table does not yet exist, create the target table.
    ```
    update_table_schema = PythonOperator(
        task_id             = 'update_table_schema'
        , python_callable   = func_update_table_schema
        , op_kwargs         = {'table' : entity.upper()}
    )
    ```
3. Merge newly ingested data from the staging layer into the raw table. Existing records with new data will be update using the entity `id` and `case_field` attributes. New records will be automatically append to the raw table. 
    ```
    merge_tables = PythonOperator(
        task_id             = 'merge_tables'
        , python_callable   = func_merge_tables
        , op_kwargs         = {
                                'entity'        : entity
                                , 'table'       : entity.upper()
                                , 'case_field'  : case_field
                                , 'entity_id'   : entity_id
                            }
    )
    ```
4. Clean up by removing the temporary table in the staging layer.
    ```
    clean_up =  PythonOperator(
        task_id             = 'clean_up'
        , python_callable   = func_job_clean_up
        , op_kwargs         = {'table' : entity.upper()}
    )
    ```
5. Each DAG will trigger another entity's DAG, if applicable.
    ```
    # check for down stream dag
    check_dag_downstream = BranchPythonOperator(
        task_id = 'check_dag_downstream'
        , python_callable = func_check_down_stream_dag
        , op_kwargs = {'down_stream_dag_id' : trigger_dag_id}
    )

    # trigger downstream/ext dag
    trigger_downstream_dag = TriggerDagRunOperator(
        task_id = 'trigger_downstream_dag'
        , trigger_dag_id = f"nba_package_{trigger_dag_id}"
        # passing values to downstream dag if needed
        , conf = {'message':''}
    )
    ```

## Design Consideration:
- Idempotent data pipeline: for the purpose of this pipeline, all tasks are idempotent. If a DAG is rerun or partially run, output will be the same as if the DAG had run successfully.

## Future Work and Improvement
- Add other endpoints
- Setup Monitoring/alert
- Add tests to data pipeline.

## Change Log:
