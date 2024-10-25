{{ config(materialized = 'view', schema = 'reporting') }}

select sum(o.linesalesamount) as sales, avg(o.margin) as margin from 

{{ ref('fct_orders') }} as o inner join {{ ref('dim_customers') }} as c

on o.customerid = c.customerid

and c.city = '{{var('v_city', 'Berlin')}}'

group by orderid

order by sales desc limit 10