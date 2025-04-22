
-- models/facts/fact_downtime.sql

{{ config(
    materialized='incremental',
    unique_key='fact_id',
    on_schema_change='append_new_columns',
    schema='dw'
) }}

with src as (
    select * from {{ ref('stg_ops_downtime') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'src.department_name',
            'src.process_name',
            'src.location_name',
            'src.process_date'
        ]) }} as fact_id,

        dd.department_id,
        dp.process_id,
        dl.location_id,
        t.time_id,
        src.downtime_hours::numeric(10,2)

    from src
    left join {{ ref('dim_department') }} dd on upper(trim(dd.department_name)) = upper(trim(src.department_name))
    left join {{ ref('dim_process') }} dp on upper(trim(dp.process_name)) = upper(trim(src.process_name))
    left join {{ ref('dim_location') }} dl on upper(trim(dl.location_name)) = upper(trim(src.location_name))
    left join {{ ref('dim_time') }} t on t.full_date = src.process_date
)

select * from final
{% if is_incremental() %}
  where t.full_date > (
    select max(t2.full_date)
    from {{ this }} f
    join {{ ref('dim_time') }} t2 on t2.time_id = f.time_id
  )
{% endif %}


