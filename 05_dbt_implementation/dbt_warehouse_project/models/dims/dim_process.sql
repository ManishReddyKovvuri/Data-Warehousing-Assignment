
-- models/dims/dim_process.sql

{{ config(
    materialized='table',schema='dw'
) }}


select
  row_number() over (order by process_name) as process_id,
  process_name
from (
select distinct
    process_name
from {{ ref('stg_ops_downtime') }}
where process_name is not null
) p