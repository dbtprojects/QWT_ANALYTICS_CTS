{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING') )}}

select * 
from {{ source('qwt_src', 'employees') }}