{{
    config(
        materialized='view'
    )
}}


select
    v.vendor_name,
    count(o.customer_id) customer_count,
    round(sum(o.gmv_local),2) total_gmv
from {{ ref('bi_test_orders') }} o
left join {{ ref('bi_test_vendors') }} v 
    on o.vendor_id = v.id
where o.country_name ='Taiwan'
group by v.vendor_name
order by customer_count desc;