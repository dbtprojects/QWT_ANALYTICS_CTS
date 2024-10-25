import snowflake.snowpark.functions as F

def model(dbt,session):

    dbt.config(materialized = "incremental", unique_key = "OrderID")

    df = dbt.source("qwt_src", "orders")

    if dbt.is_incremental:

        max_order_date = f"select max(orderdate) from {dbt.this}"

        df = df.filter(df.orderdate > session.sql(max_order_date).collect()[0][0])

    return df

        
    
