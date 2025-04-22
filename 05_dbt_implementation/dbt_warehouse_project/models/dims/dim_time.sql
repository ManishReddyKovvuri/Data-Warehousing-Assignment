{{ config(
    materialized='table',
    schema='dw'
) }}

with dates as (

    -- Generate a series of dates from 2020 to 2030
    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        interval '1 day'
    )::date as full_date

),

time_dim as (

    select
        full_date,
        extract(day from full_date)::int as day,
        extract(month from full_date)::int as month,
        extract(quarter from full_date)::int as quarter,
        extract(year from full_date)::int as year,
        extract(dow from full_date)::int in (0,6) as is_weekend
    from dates

    union

    -- Add fallback date 1957-01-01 if not already present
    select
        '1957-01-01'::date as full_date,
        1 as day,
        1 as month,
        1 as quarter,
        1957 as year,
        true as is_weekend
    where not exists (
        select 1 from dates where full_date = '1957-01-01'::date
    )

)

select
    row_number() over (order by full_date) as time_id,
    *
from time_dim
