{{ config(
    materialized='incremental',
    unique_key='fact_id',
    on_schema_change='append_new_columns',
    schema='dw'
) }}

with src as (
    select *
    from {{ ref('scd2_dim_employee') }}
    where dbt_valid_to is null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'employee_id',
            'dbt_valid_from'
        ]) }} as fact_id,

        e.employee_sk,
        t.time_id,
        e.salary,
        e.status

    from src e
    left join {{ ref('dim_time') }} t
      on t.full_date = CURRENT_DATE
)

select * from final

{% if is_incremental() %}
  where CURRENT_DATE > (
    select max(t2.full_date)
    from {{ this }} f
    join {{ ref('dim_time') }} t2 on t2.time_id = f.time_id
  )
{% endif %}
