{{
    config(
        materialized='view'
    )
}}

select 
  country_name,
  round(sum(gmv_local),2) total_gmv
from {{ ref('bi_test_orders') }}
 group by country_name;