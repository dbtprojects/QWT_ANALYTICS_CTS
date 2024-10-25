{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING') ) }}

with customers as
(
    select * from {{source('qwt_src', 'customers')}}

)

select * from customers

