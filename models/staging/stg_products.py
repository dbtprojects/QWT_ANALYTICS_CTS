def model(dbt, session):
    dbt.config(materialized = "table")
    products_source = dbt.source("qwt_src", "products")
    return products_source
