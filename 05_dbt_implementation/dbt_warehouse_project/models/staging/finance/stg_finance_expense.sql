{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'Finance_Dataset_Dirty') }}
),

clean as (
    select
        -- Employee ID cleanup
        "EmployeeID"::text as employee_id,

        -- Expense type normalization
        case
            when trim("ExpenseType") ilike 'travell' then 'Travel'
            when trim("ExpenseType") = '' or "ExpenseType" is null then 'Unknown'
            else initcap(trim("ExpenseType"))
        end as expense_type,

        -- Expense amount (convert to numeric)
        coalesce("ExpenseAmount"::numeric, 0) as expense_amount,

        -- Flag refunds
        coalesce("ExpenseAmount"::numeric, 0) < 0 as is_refund,

        -- Fix dates using macro
        {{ date_safe('"ExpenseDate"') }} as expense_date,

        -- Approved by cleanup
        coalesce(
            nullif(trim(
                case 
                    when "ApprovedBy"::text ~ '^\d+\.0$' then split_part("ApprovedBy"::text, '.', 1)
                    else "ApprovedBy"::text
                end
            ), ''),
            'UNKNOWN'
        ) as approved_by


    from src
)

select * from clean
