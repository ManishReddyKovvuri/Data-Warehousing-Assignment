{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'Operations_Dataset_Dirty') }}
),

prepped as (
    select
        upper(trim(coalesce("Department", ''))) as raw_department,
        upper(trim(coalesce("ProcessName", ''))) as raw_process,
        upper(trim(coalesce("Location", ''))) as raw_location,
        {{ date_safe('"ProcessDate"') }} as process_date,
        "DowntimeHours"::numeric as downtime_hours
    from src
),

defaults_applied as (
    select
        case when raw_department = '' or raw_department ilike 'nan' then 'UNASSIGNED_DEPT' else raw_department end as department_name,
        case when raw_process = '' or raw_process ilike 'nan' then 'UNKNOWN_PROCESS' else raw_process end as process_name,
        case when raw_location = '' or raw_location ilike 'nan' then 'UNKNOWN_LOCATION' else raw_location end as location_name,
        process_date,
        downtime_hours
    from prepped
),

avg_grouped as (
    select department_name, process_name, location_name,
           avg(downtime_hours) as avg_downtime
    from defaults_applied
    where downtime_hours is not null
    group by 1, 2, 3
),

joined as (
    select
        d.department_name,
        d.process_name,
        d.location_name,
        d.process_date,
        coalesce(d.downtime_hours, a.avg_downtime, 0) as downtime_hours
    from defaults_applied d
    left join avg_grouped a
    on d.department_name = a.department_name
    and d.process_name = a.process_name
    and d.location_name = a.location_name
)

select * from joined
