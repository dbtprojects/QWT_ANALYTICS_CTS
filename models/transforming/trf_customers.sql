{{config(materialized = 'table', schema = 'transforming')}}

select 
CUSTOMERID,
COMPANYNAME,
CONTACTNAME,
CITY,
COUNTRY,
c.DIVISIONID,
d.DivisionName,
ADDRESS,
FAX,
PHONE,
POSTALCODE,
IFF(STATEPROVINCE = '', 'NA', STATEPROVINCE) as STATEPROVINCE
from 
{{ ref('stg_customers')}} as c

inner join {{ ref('divisions') }} as d 

on c.DIVISIONID = d.DIVISIONID