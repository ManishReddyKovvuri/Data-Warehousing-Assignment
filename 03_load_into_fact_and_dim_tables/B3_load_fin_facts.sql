-- Step 0: Declare consistent job_id for the whole stage
DO $$ BEGIN
  BEGIN
    PERFORM 1 FROM temp_etl_job LIMIT 1;
  EXCEPTION WHEN undefined_table THEN
    CREATE TEMP TABLE temp_etl_job (job_id UUID);
    INSERT INTO temp_etl_job VALUES (gen_random_uuid());
  END;
END $$;


-- Step 1: Insert new expense types into dim_expense_type
INSERT INTO dw.dim_expense_type (expense_type_name)
SELECT DISTINCT UPPER(TRIM(expense_type))
FROM dw.staging_finance
WHERE UPPER(TRIM(expense_type)) NOT IN (
  SELECT UPPER(TRIM(expense_type_name)) FROM dw.dim_expense_type
);

-- Step 2: Add expense_type_id to staging if not exists
DO $$ BEGIN
    ALTER TABLE dw.staging_finance ADD COLUMN expense_type_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column expense_type_id already exists, skipping';
END $$;

-- Step 3: Map expense_type_id from dim_expense_type
UPDATE dw.staging_finance s
SET expense_type_id = d.expense_type_id
FROM dw.dim_expense_type d
WHERE UPPER(TRIM(s.expense_type)) = UPPER(TRIM(d.expense_type_name));


-- Step 4: Log unmatched employee_id values
INSERT INTO dw.data_quality_log (job_id, table_name, column_name, row_reference, original_value, issue)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'staging_finance',
  'employee_id',
  s.employee_id,
  s.employee_id,
  'EmployeeID not found in dim_employee'
FROM dw.staging_finance s
LEFT JOIN dw.dim_employee e
  ON s.employee_id = e.employee_id AND e.is_current = TRUE
WHERE e.employee_id IS NULL;



-- Step 4b: Insert audit log for unmatched employee_id values
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
FROM dw.staging_finance s
LEFT JOIN dw.dim_employee e
  ON s.employee_id = e.employee_id AND e.is_current = TRUE
WHERE e.employee_id IS NULL;

-- -- Step 5: Insert valid records into fact_expenses using employee_sk
-- INSERT INTO dw.fact_expenses (
--   employee_sk, expense_type_id, expense_amount, approved_by, time_id, is_refund
-- )
-- SELECT
--   e.employee_sk,
--   s.expense_type_id,
--   s.expense_amount,
--   s.approved_by,
--   t.time_id,
--   s.is_refund
-- FROM dw.staging_finance s
-- JOIN dw.dim_time t
--   ON t.full_date = s.expense_date::DATE
-- JOIN dw.dim_employee e
--   ON e.employee_id = s.employee_id
-- WHERE e.is_current = TRUE
--   AND NOT EXISTS (
--     SELECT 1 FROM dw.fact_expenses f
--     WHERE f.employee_sk = e.employee_sk
--       AND f.time_id = t.time_id
--       AND f.expense_type_id = s.expense_type_id
--       AND f.expense_amount = s.expense_amount
--       AND f.approved_by = s.approved_by
--       AND f.is_refund = s.is_refund
-- );


-- -- Step 6: Audit log for fact_expenses insert
-- WITH inserted_rows AS (
--   SELECT COUNT(*) AS count
--   FROM dw.fact_expenses
--   WHERE time_id = (SELECT time_id FROM dw.dim_time WHERE full_date = CURRENT_DATE)
-- )
-- INSERT INTO dw.audit_log (
--   job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
-- )
-- SELECT
--   (SELECT job_id FROM temp_etl_job),
--   'fact_expenses',
--   'load_fact_expenses',
--   r.count,
--   CASE WHEN r.count = 0 THEN 1 ELSE 0 END,
--   CASE WHEN r.count = 0 THEN 'partial' ELSE 'success' END,
--   'Inserted valid records into fact_expenses'
-- FROM inserted_rows r;

-- Step 5–6: Insert valid records into fact_expenses and audit

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
  FROM dw.staging_finance s
  JOIN dw.dim_time t
    ON t.full_date = s.expense_date::DATE
  JOIN dw.dim_employee e
    ON e.employee_id = s.employee_id AND e.is_current = TRUE
),
inserted_rows AS (
  SELECT *
  FROM candidate_rows cr
  WHERE NOT EXISTS (
    SELECT 1
    FROM dw.fact_expenses f
    WHERE f.employee_sk = cr.employee_sk
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

-- Final audit log
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
