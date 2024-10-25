{{ config(materialized = 'incremental', schema = env_var('DBT_STAGESCHEMA','STAGING') , unique_key = ['orderid', 'lineno']) }}

with orders as 
(
    select * from {{ source('qwt_src', 'orders') }}
)
,

orderdetails as 
(
    select * from {{source('qwt_src', 'orderdetails')}}
)
,

final as 
(
    select 
    od.*,
    o.orderdate

   
    from 
    orders o inner join orderdetails od on o.orderid = od.orderid
    
)

select * from final

{% if is_incremental() %}

where orderdate > (select max(orderdate) from {{source('qwt_src', 'orders')}} )

{% endif %}

