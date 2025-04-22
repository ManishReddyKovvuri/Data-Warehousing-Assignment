-- models/dims/dim_expense_type.sql

{{ config(materialized='table', schema='dw') }}


select
  row_number() over (order by expense_type_name) as expense_type_id,
  expense_type_name
from (select distinct
    expense_type as expense_type_name
from {{ ref('stg_finance_expense') }}
where expense_type is not null
) e
