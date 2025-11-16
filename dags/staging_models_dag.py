"""
DAG to run only dbt staging models.

This DAG uses Astronomer Cosmos to execute dbt models in the staging directory.
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Path to dbt project
DBT_PROJECT_PATH = Path(__file__).parent.parent
DBT_EXECUTABLE_PATH = "/usr/local/airflow/.venv/bin/dbt"

# Profile configuration - adjust based on your data warehouse
# Using Airflow connection for credentials
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake123",  # Change to your Airflow connection ID
        profile_args={
            "database": "QWT_DEV",
            "schema": "STAGING_DEV",
        },
    ),
)

# Execution configuration
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# Create the DAG
staging_dag = DbtDag(
    dag_id="run_staging_models",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    description="Run dbt staging models only",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    # Select only staging models
    operator_args={
        "select": ["path:models/staging"],
        "full_refresh": False,
    },
    tags=["dbt", "staging"],
)
