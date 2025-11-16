#!/bin/bash
# Script to set up Snowflake connection in Astro deployment
#
# Usage: ./setup_snowflake_connection.sh
#
# This script will prompt for Snowflake credentials and set them as
# environment variables in your Astro deployment.

set -e

echo "================================================"
echo "Snowflake Connection Setup for Astro Deployment"
echo "================================================"
echo ""

# Deployment ID for your project
DEPLOYMENT_ID="cmi1jcr2h0k0e01hxziutrgnk"

echo "This script will set up the Snowflake connection for:"
echo "Deployment: Astro IDE (project: QWT_ANALYTICS_CTS)"
echo "Deployment ID: $DEPLOYMENT_ID"
echo ""

# Prompt for Snowflake credentials
read -p "Enter Snowflake Account (e.g., xy12345.us-east-1): " SF_ACCOUNT
read -p "Enter Snowflake User: " SF_USER
read -sp "Enter Snowflake Password: " SF_PASSWORD
echo ""
read -p "Enter Snowflake Warehouse (default: COMPUTE_WH): " SF_WAREHOUSE
SF_WAREHOUSE=${SF_WAREHOUSE:-COMPUTE_WH}
read -p "Enter Snowflake Role (default: SYSADMIN): " SF_ROLE
SF_ROLE=${SF_ROLE:-SYSADMIN}
read -p "Enter Snowflake Database (default: QWT_ANALYTICS): " SF_DATABASE
SF_DATABASE=${SF_DATABASE:-QWT_ANALYTICS}
read -p "Enter Snowflake Schema (default: STAGING): " SF_SCHEMA
SF_SCHEMA=${SF_SCHEMA:-STAGING}

echo ""
echo "================================================"
echo "Configuration Summary:"
echo "================================================"
echo "Account:   $SF_ACCOUNT"
echo "User:      $SF_USER"
echo "Warehouse: $SF_WAREHOUSE"
echo "Role:      $SF_ROLE"
echo "Database:  $SF_DATABASE"
echo "Schema:    $SF_SCHEMA"
echo ""

read -p "Proceed with deployment? (y/n): " CONFIRM

if [[ $CONFIRM != "y" && $CONFIRM != "Y" ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Setting up Snowflake connection..."
echo ""

# Build connection string
CONN_STRING="snowflake://${SF_USER}:${SF_PASSWORD}@${SF_ACCOUNT}/?database=${SF_DATABASE}&warehouse=${SF_WAREHOUSE}&role=${SF_ROLE}&schema=${SF_SCHEMA}"

# Create environment variable for Airflow connection
echo "Creating AIRFLOW_CONN_SNOWFLAKE_DEFAULT variable..."
astro deployment variable create \
  --deployment-id "$DEPLOYMENT_ID" \
  --key "AIRFLOW_CONN_SNOWFLAKE_DEFAULT" \
  --value "$CONN_STRING" \
  --secret

echo "✓ AIRFLOW_CONN_SNOWFLAKE_DEFAULT created"

# Set dbt environment variables
echo ""
echo "Setting dbt environment variables..."

astro deployment variable create \
  --deployment-id "$DEPLOYMENT_ID" \
  --key "DBT_SOURCEDB" \
  --value "$SF_DATABASE" || echo "DBT_SOURCEDB already exists (skipping)"

astro deployment variable create \
  --deployment-id "$DEPLOYMENT_ID" \
  --key "DBT_STAGESCHEMA" \
  --value "STAGING" || echo "DBT_STAGESCHEMA already exists (skipping)"

astro deployment variable create \
  --deployment-id "$DEPLOYMENT_ID" \
  --key "DBT_AUDITSCHEMA" \
  --value "AUDITING" || echo "DBT_AUDITSCHEMA already exists (skipping)"

echo ""
echo "================================================"
echo "✓ Setup Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Deploy your DAGs: astro deploy"
echo "2. Test the connection using the 'test_snowflake_connection' DAG"
echo "3. Run the 'dbt_staging_models' DAG"
echo ""
