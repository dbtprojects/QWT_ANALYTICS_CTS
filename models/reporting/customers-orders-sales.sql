{{ config(materialized = 'view', schema = 'reporting') }}

with customers as
(
    select 
    customerid as customerid,
    companyname as companyname,
    contactname as contactname,
    city as city

    from {{ ref("dim_customers") }}

),

orders as 
(
    select 
    customerid as customerid,
    orderid as orderid,
    linesalesamount as sales,
    orderdate as orderdate
    from {{ref('fct_orders')}}

),

customer_orders as

(
    select customers.companyname,
    customers.contactname,
    customers.city,
    min(orders.orderdate) as first_order_date,
    max(orders.orderdate) as recent_order_date,
    count(orders.orderid) as number_of_orders,
    sum(orders.sales) as total_sales

    from customers inner join orders on customers.customerid = orders.customerid
    
    group by customers.companyname, customers.contactname, customers.city

    order by total_sales desc
)


select * from customer_orders