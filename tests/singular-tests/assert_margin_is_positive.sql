select 
orderid,
avg(margin) as avg_margin
from 

{{ref('fct_orders')}}

group by orderid

having not (avg_margin >=0 )