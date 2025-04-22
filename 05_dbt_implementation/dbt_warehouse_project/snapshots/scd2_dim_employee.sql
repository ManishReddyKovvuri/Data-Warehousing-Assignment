{% snapshot scd2_dim_employee %}
{{
    config(
      target_schema='dw',
      unique_key='employee_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

with src as (
  select
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk,
    e.employee_id,
    e.name,
    e.gender,
    e.date_of_joining,
    e.manager_id,
    d.department_id,
    e.salary,
    e.status,
    current_timestamp as updated_at
  from {{ ref('stg_hr_employee') }} e
  left join {{ ref('dim_department') }} d
    on upper(trim(e.department_name)) = upper(trim(d.department_name))
)

select * from src

{% endsnapshot %}
