{{ config(materialized = 'table', schema = 'transforming') }}

select 
emp.empid,
emp.lastname,
emp.firstname,
emp.title,
emp.hiredate,
mgr.firstname as managername,
mgr.title as managerrole,
o.address as officeaddress,
o.city as officecity,
o.country as officecountry,
emp.yearsalary

from {{ref('stg_employees')}} as emp inner join {{ref('stg_employees')}} as mgr

on emp.reportsto = mgr.empid 

inner join {{ref('stg_offices')}} as o on
emp.office = o.Officeid
