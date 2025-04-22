-- models/facts/fact_expenses.sql

{{ config(
    materialized='incremental',
    unique_key='fact_id',
    on_schema_change='append_new_columns',
    schema='dw'
) }}

with src as (
    select * from {{ ref('stg_finance_expense') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'src.employee_id',
            'src.expense_date',
            'src.expense_amount'
        ]) }} as fact_id,

        e.employee_sk,
        et.expense_type_id,
        src.expense_amount,
        src.approved_by,
        t.time_id,
        src.is_refund

    from src
    left join {{ ref('scd2_dim_employee') }} e
      on e.employee_id = src.employee_id and e.dbt_valid_to is null
    left join {{ ref('dim_expense_type') }} et
      on et.expense_type_name = src.expense_type
    left join {{ ref('dim_time') }} t
      on t.full_date = src.expense_date
)

select * from final
{% if is_incremental() %}
  where t.full_date > (
    select max(t2.full_date)
    from {{ this }} f
    join {{ ref('dim_time') }} t2 on t2.time_id = f.time_id
  )
{% endif %}

