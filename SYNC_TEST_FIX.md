# Sync/Test Failure - Root Causes and Fixes

## Problems Identified

### 1. ❌ Missing Astro Project Files (CRITICAL)
Your project was missing essential Astro project configuration files that are required for deployment.

**What was missing:**
- `Dockerfile` - Defines the Airflow runtime image
- `.astro/config.yaml` - Astro project configuration
- `.dockerignore` - Prevents unnecessary files from being built into the image
- `packages.txt` - System-level package dependencies

**Why this caused failure:**
Astro CLI and Astro Cloud require these files to build and deploy your project. Without them, the sync/test process cannot proceed.

### 2. ⚠️ DAG Selection Issue (FIXED)
The DAG was using `select=["+tag:staging"]` but your dbt models don't have a `staging` tag defined.

**What I changed:**
```python
# Before:
select=["+tag:staging"]

# After:
select=["path:models/staging"]
```

**Why this matters:**
Path-based selection is more reliable and doesn't require tags to be defined in your dbt models.

---

## Files Created/Fixed

### ✅ Created Files:

1. **`Dockerfile`**
   ```dockerfile
   FROM quay.io/astronomer/astro-runtime:12.0.0
   ```
   - Uses Astro Runtime 12.0.0 (matches your deployment)
   - Airflow 2.10.0 included

2. **`.astro/config.yaml`**
   ```yaml
   project:
     name: QWT_ANALYTICS_CTS
   cloud:
     organization_id: cmi04t8b6080301hxp94bikx0
   ```
   - Links project to your Astro organization

3. **`.dockerignore`**
   - Excludes unnecessary files from Docker build
   - Includes .git, target/, dbt_packages/, __pycache__, etc.

4. **`packages.txt`**
   - Placeholder for system-level dependencies
   - Add any OS packages needed (e.g., git, build-essential)

### ✅ Modified Files:

1. **`dags/dbt_staging_models_dag.py`**
   - Changed from tag-based to path-based model selection
   - Now uses `select=["path:models/staging"]`

---

## How to Verify the Fix

### Method 1: Test Locally (Recommended First)
```bash
# 1. Start Astro locally
astro dev start

# 2. Check if DAGs appear without errors
# Open: http://localhost:8080
# Login: admin / admin

# 3. Look for the DAG in the UI
# - dbt_staging_models should appear
# - Check for any import errors
```

### Method 2: Deploy to Astro Cloud
```bash
# Deploy to your Astro deployment
astro deploy

# Monitor the deployment
# Go to: https://cloud.astronomer.io
# Check deployment status
```

---

## Additional Issues That Could Still Cause Problems

### 1. Missing Snowflake Connection
**Symptom:** DAG appears but fails when triggered
**Solution:** Follow the `SNOWFLAKE_CONNECTION_SETUP.md` guide

### 2. Missing dbt Dependencies
**Symptom:** dbt commands fail with "package not found"
**Solution:** The DAG has `install_deps: True` so this should auto-resolve

### 3. Database/Schema Don't Exist
**Symptom:** dbt models fail with "database not found"
**Solution:** Create in Snowflake:
```sql
CREATE DATABASE IF NOT EXISTS QWT_DEV;
CREATE SCHEMA IF NOT EXISTS QWT_DEV.STAGING_DEV;
CREATE SCHEMA IF NOT EXISTS QWT_DEV.RAW;
CREATE SCHEMA IF NOT EXISTS QWT_DEV.AUDITING;
```

### 4. Source Tables Don't Exist
**Symptom:** Staging models fail with "source not found"
**Solution:**
- Verify tables exist in `QWT_DEV.RAW` schema
- Or update `models/staging/qwt-sources.yml` to point to correct database/schema

---

## Project Structure (After Fix)

```
tmpluvlbazj/
├── .astro/
│   └── config.yaml          ✅ NEW
├── .dockerignore            ✅ NEW
├── .gitignore
├── Dockerfile               ✅ NEW
├── README.md
├── SNOWFLAKE_CONNECTION_SETUP.md
├── SYNC_TEST_FIX.md        ✅ NEW
├── dags/
│   └── dbt_staging_models_dag.py  ✅ FIXED
├── dbt_project.yml
├── models/
│   └── staging/
│       ├── stg_customers.sql
│       ├── stg_employees.sql
│       ├── stg_offices.sql
│       ├── stg_orderdetails.sql
│       ├── stg_shipments.sql
│       └── stg_suppliers.sql
├── packages.txt             ✅ NEW
├── packages.yml             (dbt)
├── requirements.txt
├── seeds/
├── snapshots/
└── tests/
```

---

## Next Steps

1. **Test locally first:**
   ```bash
   astro dev start
   ```

2. **Add Snowflake connection** (see SNOWFLAKE_CONNECTION_SETUP.md)

3. **Deploy to Astro Cloud:**
   ```bash
   astro deploy
   ```

4. **Verify DAG appears in Airflow UI**

5. **Test the connection** using `test_snowflake_connection` DAG

6. **Run staging models** by triggering `dbt_staging_models` DAG

---

## Troubleshooting

### If sync/test still fails:

1. **Check DAG syntax:**
   - Look for Python syntax errors in DAG files
   - Check import statements

2. **Verify requirements.txt:**
   - Ensure all packages are installable
   - Check for version conflicts

3. **Check Astro CLI version:**
   ```bash
   astro version
   ```
   Update if needed: `astro upgrade`

4. **Check deployment logs:**
   - In Astro Cloud UI, go to your deployment
   - Click "Logs" tab
   - Look for build or parsing errors

5. **Clear local environment:**
   ```bash
   astro dev stop
   astro dev kill
   astro dev start --clean
   ```

---

## Summary

**Root Cause:** Missing Astro project configuration files (Dockerfile, .astro/config.yaml, etc.)

**Fix Applied:** Created all required Astro project files + fixed DAG model selection

**Status:** ✅ Project should now sync/test successfully

**Note:** You still need to configure the Snowflake connection before running the DAG.
