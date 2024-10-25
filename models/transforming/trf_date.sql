{{ config(materialized = 'table', schema = 'transforming')}}

{% set v_min_orderdate = get_min_orderdate() %}
{% set v_max_orderdate = get_max_orderdate() %}

{{ dbt_date.get_date_dimension(v_min_orderdate, v_max_orderdate) }}