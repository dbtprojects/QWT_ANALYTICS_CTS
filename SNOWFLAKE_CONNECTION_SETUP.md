# Snowflake Connection Setup Guide

## Option 1: Astro CLI with Local Airflow UI (For Local Development)

### Step 1: Start Astro locally
```bash
astro dev start
```

### Step 2: Access Airflow UI
- Open: http://localhost:8080
- Login: admin / admin (default credentials)

### Step 3: Add Snowflake Connection
1. Go to **Admin** → **Connections**
2. Click **+** (Add a new record)
3. Fill in the following details:

**Connection Details:**
- **Connection Id**: `snowflake_default`
- **Connection Type**: `Snowflake`
- **Account**: `<your-snowflake-account>` (e.g., `xy12345.us-east-1`)
- **User**: `<your-snowflake-username>`
- **Password**: `<your-snowflake-password>`
- **Database**: `QWT_ANALYTICS` (or your source database)
- **Schema**: `STAGING` (default schema)
- **Warehouse**: `<your-warehouse-name>` (e.g., `COMPUTE_WH`)
- **Role**: `<your-role>` (e.g., `ACCOUNTADMIN`, `SYSADMIN`)
- **Region**: `<your-region>` (if applicable, e.g., `us-east-1`)

4. Click **Test** to verify the connection
5. Click **Save**

---

## Option 2: Astro Cloud via Environment Variables (Recommended for Production)

This approach uses environment variables and is best for production deployments.

### Step 1: Set Environment Variables in Astro Deployment

Using Astro CLI:
```bash
# Set Snowflake credentials as environment variables
astro deployment variable create \
  --deployment-id cmi1jcr2h0k0e01hxziutrgnk \
  --key AIRFLOW_CONN_SNOWFLAKE_DEFAULT \
  --value "snowflake://<user>:<password>@<account>/?database=QWT_ANALYTICS&warehouse=COMPUTE_WH&role=SYSADMIN&schema=STAGING" \
  --secret
```

Or via Astro Cloud UI:
1. Go to https://cloud.astronomer.io
2. Navigate to your Deployment: **Astro IDE (project: QWT_ANALYTICS_CTS)**
3. Go to **Environment** tab
4. Add environment variable:
   - **Key**: `AIRFLOW_CONN_SNOWFLAKE_DEFAULT`
   - **Value**: `snowflake://<user>:<password>@<account>/?database=QWT_ANALYTICS&warehouse=COMPUTE_WH&role=SYSADMIN&schema=STAGING`
   - **Mark as Secret**: ✓ (checked)
5. Click **Save**

**Connection String Format:**
```
snowflake://<user>:<password>@<account>/?database=<db>&warehouse=<wh>&role=<role>&schema=<schema>
```

**Example:**
```
snowflake://myuser:mypass123@xy12345.us-east-1/?database=QWT_ANALYTICS&warehouse=COMPUTE_WH&role=SYSADMIN&schema=STAGING
```

---

## Option 3: Using connections.yaml File (For Multiple Environments)

Create a `connections.yaml` file in your project for version-controlled connection templates.

### Step 1: Create connections.yaml
```bash
# In your project root
touch connections.yaml
```

### Step 2: Add Snowflake connection template
```yaml
# connections.yaml
snowflake_default:
  conn_type: snowflake
  host: ''
  login: ${SNOWFLAKE_USER}  # Reference environment variable
  password: ${SNOWFLAKE_PASSWORD}  # Reference environment variable
  schema: STAGING
  extra:
    account: ${SNOWFLAKE_ACCOUNT}
    warehouse: ${SNOWFLAKE_WAREHOUSE}
    database: QWT_ANALYTICS
    role: ${SNOWFLAKE_ROLE}
    region: ${SNOWFLAKE_REGION}
```

### Step 3: Set environment variables in Astro
```bash
astro deployment variable create --key SNOWFLAKE_USER --value "your_user" --secret
astro deployment variable create --key SNOWFLAKE_PASSWORD --value "your_password" --secret
astro deployment variable create --key SNOWFLAKE_ACCOUNT --value "xy12345.us-east-1" --secret
astro deployment variable create --key SNOWFLAKE_WAREHOUSE --value "COMPUTE_WH"
astro deployment variable create --key SNOWFLAKE_ROLE --value "SYSADMIN"
astro deployment variable create --key SNOWFLAKE_REGION --value "us-east-1"
```

---

## Option 4: Using Astro Cloud UI (Simplest for Production)

### Step 1: Access Astro Cloud
1. Go to https://cloud.astronomer.io
2. Navigate to your Deployment: **Astro IDE (project: QWT_ANALYTICS_CTS)**

### Step 2: Add Connection via UI
1. Click on **Connections** tab
2. Click **+ Add Connection**
3. Fill in the form:
   - **Connection ID**: `snowflake_default`
   - **Connection Type**: `Snowflake`
   - **Account**: `<your-account>`
   - **User**: `<your-user>`
   - **Password**: `<your-password>`
   - **Database**: `QWT_ANALYTICS`
   - **Schema**: `STAGING`
   - **Warehouse**: `<your-warehouse>`
   - **Role**: `<your-role>`
4. Click **Test Connection**
5. Click **Save**

---

## Verifying Your Connection

### Test in Airflow UI
1. Go to **Admin** → **Connections**
2. Find `snowflake_default`
3. Click the **Test** button

### Test with a Simple DAG
Create a test DAG to verify:

```python
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

@dag(
    dag_id="test_snowflake_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def test_snowflake():
    @task
    def test_connection():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        result = cursor.fetchone()
        print(f"Connected as: {result[0]}, Database: {result[1]}, Warehouse: {result[2]}")
        return result

    test_connection()

test_snowflake()
```

---

## Environment Variables Used by Your dbt Project

Make sure these are set in your Astro deployment:

- `DBT_SOURCEDB` (default: `QWT_ANALYTICS`) - Source database
- `DBT_STAGESCHEMA` (default: `STAGING`) - Staging schema
- `DBT_AUDITSCHEMA` (default: `AUDITING`) - Auditing schema

Set them via:
```bash
astro deployment variable create --key DBT_SOURCEDB --value "QWT_ANALYTICS"
astro deployment variable create --key DBT_STAGESCHEMA --value "STAGING"
astro deployment variable create --key DBT_AUDITSCHEMA --value "AUDITING"
```

---

## Troubleshooting

### Common Issues

1. **"Account must be specified"**: Make sure your account identifier is correct
   - Format: `<account_locator>.<region>` (e.g., `xy12345.us-east-1`)
   - Or use organization-based format: `<orgname>-<account_name>`

2. **"Invalid warehouse"**: Ensure the warehouse exists and you have access

3. **"Database does not exist"**: Create the database in Snowflake first
   ```sql
   CREATE DATABASE IF NOT EXISTS QWT_ANALYTICS;
   CREATE SCHEMA IF NOT EXISTS QWT_ANALYTICS.STAGING;
   CREATE SCHEMA IF NOT EXISTS QWT_ANALYTICS.AUDITING;
   ```

4. **Authentication errors**: Check username/password and role permissions

---

## Next Steps

1. Add the Snowflake connection using one of the methods above
2. Deploy your DAG: `astro deploy`
3. Trigger the DAG: `dbt_staging_models`
4. Monitor execution in Airflow UI

For more information:
- [Astronomer Connection Management](https://docs.astronomer.io/astro/manage-connections)
- [Snowflake Provider Docs](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html)
