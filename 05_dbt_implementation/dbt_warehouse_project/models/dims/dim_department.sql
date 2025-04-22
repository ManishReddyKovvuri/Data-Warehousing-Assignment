{{ config(materialized='table', schema='dw') }}

select
  row_number() over (order by department_name) as department_id,
  department_name
from (
  select distinct department_name
  from {{ ref('stg_hr_employee') }}
  where department_name is not null
) d