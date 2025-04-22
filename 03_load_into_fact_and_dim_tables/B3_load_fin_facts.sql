-- Step 0: Declare consistent job_id for the whole stage
DO $$ BEGIN
  BEGIN
    PERFORM 1 FROM temp_etl_job LIMIT 1;
  EXCEPTION WHEN undefined_table THEN
    CREATE TEMP TABLE temp_etl_job (job_id UUID);
    INSERT INTO temp_etl_job VALUES (gen_random_uuid());
  END;
END $$;


--  Insert new expense types into dim_expense_type
INSERT INTO dw.dim_expense_type (expense_type_name)
SELECT DISTINCT UPPER(TRIM(expense_type))
FROM stg.staging_finance
WHERE UPPER(TRIM(expense_type)) NOT IN (
  SELECT UPPER(TRIM(expense_type_name)) FROM dw.dim_expense_type
);

-- Add expense_type_id to staging if not exists
DO $$ BEGIN
    ALTER TABLE stg.staging_finance ADD COLUMN expense_type_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column expense_type_id already exists, skipping';
END $$;

-- Map expense_type_id from dim_expense_type
UPDATE stg.staging_finance s
SET expense_type_id = d.expense_type_id
FROM dw.dim_expense_type d
WHERE UPPER(TRIM(s.expense_type)) = UPPER(TRIM(d.expense_type_name));


-- Log unmatched employee_id values
INSERT INTO dw.data_quality_log (job_id, table_name, column_name, row_reference, original_value, issue)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'staging_finance',
  'employee_id',
  s.employee_id,
  s.employee_id,
  'EmployeeID not found in dim_employee'
FROM stg.staging_finance s
LEFT JOIN dw.dim_employee e
  ON s.employee_id = e.employee_id AND e.is_current = TRUE
WHERE e.employee_id IS NULL;



--  Insert audit log for unmatched employee_id values
INSERT INTO dw.audit_log (
  job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'staging_finance',
  'validate_employee_fk',
  COUNT(*),
  COUNT(*),
  CASE WHEN COUNT(*) = 0 THEN 'success' ELSE 'partial' END,
  'EmployeeID lookup validation against dim_employee completed'
FROM stg.staging_finance s
LEFT JOIN dw.dim_employee e
  ON s.employee_id = e.employee_id AND e.is_current = TRUE
WHERE e.employee_id IS NULL;

-- Define candidate + inserted rows
WITH candidate_rows AS (
  SELECT
    s.employee_id,
    s.expense_type_id,
    s.expense_amount,
    s.approved_by,
    t.time_id,
    s.is_refund,
    e.employee_sk
  FROM stg.staging_finance s
  JOIN dw.dim_time t
    ON t.full_date = s.expense_date::DATE
  JOIN dw.dim_employee e
    ON e.employee_id = s.employee_id AND e.is_current = TRUE
),inserted_rows AS (
  SELECT *
  FROM candidate_rows cr
  WHERE NOT EXISTS (
    SELECT 1
    FROM dw.fact_expenses f
    JOIN dw.dim_employee e ON f.employee_sk = e.employee_sk
    WHERE e.employee_id = cr.employee_id
      AND f.time_id = cr.time_id
      AND f.expense_type_id = cr.expense_type_id
      AND f.expense_amount = cr.expense_amount
      AND f.approved_by = cr.approved_by
      AND f.is_refund = cr.is_refund
  )
),
do_insert AS (
  INSERT INTO dw.fact_expenses (
    employee_sk, expense_type_id, expense_amount, approved_by, time_id, is_refund
  )
  SELECT
    employee_sk, expense_type_id, expense_amount, approved_by, time_id, is_refund
  FROM inserted_rows
  RETURNING *
)

--  audit log
INSERT INTO dw.audit_log (
  job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'fact_expenses',
  'load_fact_expenses',
  (SELECT COUNT(*) FROM do_insert),
  (SELECT COUNT(*) FROM candidate_rows) - (SELECT COUNT(*) FROM do_insert),
  CASE
    WHEN (SELECT COUNT(*) FROM candidate_rows) = 0 THEN 'failed'
    WHEN (SELECT COUNT(*) FROM do_insert) = (SELECT COUNT(*) FROM candidate_rows) THEN 'success'
    ELSE 'partial'
  END,
  FORMAT(
    'Attempted: %s, Inserted: %s, Skipped: %s due to duplicates',
    (SELECT COUNT(*) FROM candidate_rows),
    (SELECT COUNT(*) FROM do_insert),
    (SELECT COUNT(*) FROM candidate_rows) - (SELECT COUNT(*) FROM do_insert)
  );
