{{
    config(
        materialized='view'
    )
}}


select
  country_name,
  vendor_name,
  total_gmv
from
(select
  o.country_name,
  v.vendor_name,
  round(sum(o.gmv_local),2) total_gmv,
  rank() over(partition by o.country_name order by round(sum(o.gmv_local),2) desc) as rnk
from {{ ref('bi_test_orders') }} o
left join {{ ref('bi_test_vendors') }} v 
    on o.vendor_id = v.id
group by o.country_name,v.vendor_name
)
where rnk = 1
order by country_name
