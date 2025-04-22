{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'HR_Dataset_Dirty') }}
),

clean as (
    select
        -- Generate surrogate key for SCD2
        {{ dbt_utils.generate_surrogate_key(['"EmployeeID"']) }} as employee_key,

        -- Department cleanup
        coalesce(nullif(upper(trim("Department")), ''), 'UNASSIGNED_DEPT') as department_name,

        -- Gender normalization and fallback
        case
            when upper(trim("Gender")) in ('M', 'MALE') then 'M'
            when upper(trim("Gender")) in ('F', 'FEMALE') then 'F'
            else 'UNKNOWN'
        end as gender,

        -- Fix DateOfJoining using macro
        cast({{ date_safe('"DateOfJoining"') }} as date) as date_of_joining,

        -- Trim and cast ManagerID
        nullif(trim("ManagerID"::text), '') as manager_id,

        -- Salary: numeric conversion, default 0, and abs
        abs(coalesce("Salary"::numeric, 0)) as salary,

        -- Status normalization
        case
            when upper(trim("Status")) = 'ACTIVE' then 'Active'
            when upper(trim("Status")) = 'RESIGNED' then 'Resigned'
            else 'Unknown'
        end as status,

        -- Fallback Name
        coalesce(nullif(trim("Name"), ''), concat('EMP_', "EmployeeID")) as name,

        -- Fallback Employee ID if missing
        coalesce("EmployeeID"::text, concat('TEMP_', row_number() over ())) as employee_id

    from src
)

select * from clean
