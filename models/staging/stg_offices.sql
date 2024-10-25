{{config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING') )}}

select 
office as Officeid,
OFFICEADDRESS as address,
OFFICEPOSTALCODE as POSTALCODE,
OFFICECITY as CITY,
OFFICESTAEPROVINCE as STATEPROVINCE,
OFFICEPHONE as phone,
OFFICEFAX as FAX,
OFFICECOUNTRY as COUNTRY

from 

{{ source('qwt_src', 'offices')}}