"""
DAG for running dbt staging models.

This DAG orchestrates the execution of dbt staging models from the QWT_ANALYTICS_CTS project.
It runs all models in the models/staging directory using the Cosmos library for dbt-Airflow integration.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# dbt project path (relative to the Airflow dags folder)
DBT_PROJECT_PATH = Path(__file__).parent.parent
DBT_EXECUTABLE_PATH = "/usr/local/airflow/.venv/bin/dbt"


@dag(
    dag_id="dbt_staging_models",
    default_args=default_args,
    description="Run dbt staging models for QWT Analytics",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "staging", "qwt_analytics"],
)
def dbt_staging_models_dag():
    """
    DAG to run dbt staging models.

    The staging models extract data from source tables and apply basic transformations.
    Models included:
    - stg_customers
    - stg_orders
    - stg_orderdetails
    - stg_employees
    - stg_offices
    - stg_shipments
    - stg_suppliers
    """

    # Configure dbt profile for Snowflake connection
    profile_config = ProfileConfig(
        profile_name="default",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake123",
            profile_args={
                "database": "QWT_DEV",
                "schema": "STAGING_DEV",
            },
        ),
    )

    # Configure dbt project
    project_config = ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    )

    # Configure execution settings
    execution_config = ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )

    # Create dbt task group for staging models only
    staging_models = DbtTaskGroup(
        group_id="staging_models",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,  # Install dbt packages before running
        },
        # Select only staging models using path-based selection
        select=["path:models/staging"],
    )

    staging_models


# Instantiate the DAG
dbt_staging_dag = dbt_staging_models_dag()
