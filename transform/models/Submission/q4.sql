{{
    config(
        materialized='view'
    )
}}


select
      year,
     country_name,
     vendor_name,
      total_gmv
from
(select
  date_trunc(o.date_local,year) year,
  o.country_name,
  v.vendor_name,
  round(sum(o.gmv_local),2) total_gmv,
  rank() over(partition by date_trunc(o.date_local,year),o.country_name order by round(sum(o.gmv_local),2) desc) as rnk
from `bi_test.orders` o
left join `bi_test.vendors` v 
    on o.vendor_id = v.id
group by o.date_local,o.country_name,v.vendor_name
)
where rnk in (1,2)
order by year,country_name
