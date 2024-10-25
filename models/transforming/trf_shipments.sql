{{ config(materialized = 'table', schema = 'transforming') }}

select 
sh.orderid,
sh.shipmentdate,
sh.status,
sp.CompanyName as Companyname

from 

{{ref('shipments_snapshot')}} as sh inner join {{ref('shippers')}} as sp on sh.shipperid = sp.shipperid

where sh.dbt_valid_to is null