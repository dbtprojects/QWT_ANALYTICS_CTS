{{ config(materialized = 'table', schema = 'transforming') }}

select 

p.productid,
p.productname,
c.categoryname,
s.companyname,
s.contactname,
s.city,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
IFF(p.unitsinstock - p.unitsonorder > 0, 'Available', 'Not Available') as ProductAvailability

from 

{{ref('stg_products')}} as p inner join {{ref('trf_suppliers')}} as s 
on p.supplierid = s.supplierid inner join {{ref('categories')}} as c 
on c.categoryid = p.categoryid