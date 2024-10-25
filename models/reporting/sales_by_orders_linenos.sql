{{ config(materialized = 'view', schema = 'reporting') }}

{% set order_linenos = get_order_linenos() %}

select
orderid,
{% for line_numbers in order_linenos %}
sum(case when lineno =  {{line_numbers}} then linesalesamount end) as lineno{{line_numbers}}_sales,
{% endfor %}
sum(linesalesamount) as totalsales
from {{ ref('fct_orders') }}
group by 1
