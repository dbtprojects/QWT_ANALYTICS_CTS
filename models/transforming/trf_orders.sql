{{ config(materialized = 'table', schema = 'transforming')}}

select 

o.orderid as orderid,
o.orderdate as orderdate,
year(o.orderdate) as orderyear,
month(o.orderdate) as ordermonth,
o.CUSTOMERID as customerid,
o.employeeid as employeeid,
o.shipperid as shipperid,
od.lineno as lineno,
od.productid as productid,
od.quantity as quantity,
od.discount as discount,
od.unitprice as unitprice,
o.freight as freight,
(od.UnitPrice * od.Quantity) * (1-od.Discount) as linesalesamount,
p.UnitCost * od.Quantity as costofgoodssold,
((od.UnitPrice * od.Quantity) * (1-od.Discount)) - (p.UnitCost * od.Quantity) as margin

from 

{{ref('stg_orders')}} as o inner join {{ref('stg_orderdetails')}} as od 
on o.orderid = od.orderid 
inner join {{ref('stg_products')}} as p on od.productid = p.productid