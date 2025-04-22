
-- models/dims/dim_location.sql

{{ config(
    materialized='table',schema='dw'
) }}


select
  row_number() over (order by location_name) as location_id,
  location_name
from (
select distinct
    location_name
from {{ ref('stg_ops_downtime') }}
where location_name is not null
) l
