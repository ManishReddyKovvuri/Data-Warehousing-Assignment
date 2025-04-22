-- A3_load_dim_emp

--  Ensure job_id table exists
DO $$
BEGIN
  BEGIN
    PERFORM 1 FROM temp_etl_job LIMIT 1;
  EXCEPTION WHEN undefined_table THEN
    CREATE TEMP TABLE temp_etl_job (job_id UUID);
    INSERT INTO temp_etl_job VALUES (gen_random_uuid());
  END;
END $$;

--  Insert new departments from staging to dim_department
INSERT INTO dw.dim_department (department_name)
SELECT DISTINCT UPPER(TRIM("Department"))
FROM dw.staging_employee
WHERE UPPER(TRIM("Department")) NOT IN (
  SELECT UPPER(TRIM(department_name)) FROM dw.dim_department
);

--  Add row_hash to staging if not exists
DO $$ BEGIN
    ALTER TABLE dw.staging_employee ADD COLUMN row_hash TEXT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column row_hash already exists, skipping';
END $$;

UPDATE dw.staging_employee
SET row_hash = md5(concat_ws('::', "Name", "Gender", "DateOfJoining", "ManagerID", "Department", "Salary", "Status"));

--  Map department_id from dim_department into staging_employee
DO $$ BEGIN
    ALTER TABLE dw.staging_employee ADD COLUMN department_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'department_id already exists, skipping';
END $$;

UPDATE dw.staging_employee s
SET department_id = d.department_id
FROM dw.dim_department d
WHERE UPPER(s."Department") = UPPER(d.department_name);

--  Expire existing dim_employee rows that have changed
UPDATE dw.dim_employee d
SET valid_to = CURRENT_DATE,
    is_current = FALSE
FROM dw.staging_employee s
WHERE d.employee_id::TEXT = s."EmployeeID"::TEXT
  AND d.is_current = TRUE
  AND d.row_hash IS DISTINCT FROM s.row_hash;

-- Insert new or changed rows (SCD2 logic with CASE for valid_from)
INSERT INTO dw.dim_employee (
  employee_id, name, gender, date_of_joining, manager_id,
  department_id, row_hash, valid_from, valid_to, is_current
)
SELECT
  s."EmployeeID"::TEXT, s."Name", s."Gender", s."DateOfJoining"::DATE, s."ManagerID"::TEXT,
  s.department_id, s.row_hash, CURRENT_DATE, NULL, TRUE
FROM dw.staging_employee s
LEFT JOIN dw.dim_employee d
  ON s."EmployeeID"::TEXT = d.employee_id::TEXT AND d.is_current = TRUE
WHERE d.row_hash IS DISTINCT FROM s.row_hash OR d.row_hash IS NULL;


-- Audit log for dim_employee insert
WITH inserted_rows AS (
  SELECT COUNT(*) AS count
  FROM dw.dim_employee
  WHERE valid_from = CURRENT_DATE
)
INSERT INTO dw.audit_log (
  job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'dim_employee',
  'dim_load',
  r.count,
  CASE WHEN r.count = 0 THEN 1 ELSE 0 END,
  CASE WHEN r.count = 0 THEN 'partial' ELSE 'success' END,
  'SCD Type 2 load applied to dim_employee'
FROM inserted_rows r;


--  Load snapshot into fact_employee
INSERT INTO dw.fact_employee (
  employee_sk, time_id, salary, status
)
SELECT
  e.employee_sk,
  t.time_id,
  s."Salary",
  s."Status"
FROM dw.dim_employee e
JOIN dw.staging_employee s
  ON e.employee_id::TEXT = s."EmployeeID"::TEXT
  AND e.is_current = TRUE
JOIN dw.dim_time t
  ON t.full_date = CURRENT_DATE;

--  Audit log for fact_employee snapshot
WITH inserted_rows AS (
  SELECT COUNT(*) AS count
  FROM dw.fact_employee
  WHERE time_id = (SELECT time_id FROM dw.dim_time WHERE full_date = CURRENT_DATE)
)
INSERT INTO dw.audit_log (
  job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'fact_employee',
  'load_fact_employee_snapshot',
  r.count,
  CASE WHEN r.count = 0 THEN 1 ELSE 0 END,
  CASE WHEN r.count = 0 THEN 'partial' ELSE 'success' END,
  'Inserted snapshot records into fact_employee'
FROM inserted_rows r;


--02_load_dim_fact_finance

--  Insert new expense types into dim_expense_type
INSERT INTO dw.dim_expense_type (expense_type_name)
SELECT DISTINCT UPPER(TRIM(expense_type))
FROM dw.staging_finance
WHERE UPPER(TRIM(expense_type)) NOT IN (
  SELECT UPPER(TRIM(expense_type_name)) FROM dw.dim_expense_type
);

-- Add expense_type_id to staging if not exists
DO $$ BEGIN
    ALTER TABLE dw.staging_finance ADD COLUMN expense_type_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column expense_type_id already exists, skipping';
END $$;

-- Map expense_type_id from dim_expense_type
UPDATE dw.staging_finance s
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
FROM dw.staging_finance s
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
FROM dw.staging_finance s
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
  FROM dw.staging_finance s
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



-- 03_load_dim_fact_operations

--  Insert new process names into dim_process
INSERT INTO dw.dim_process (process_name)
SELECT DISTINCT process_name
FROM dw.staging_operations
WHERE process_name NOT IN (
  SELECT process_name FROM dw.dim_process
);

INSERT INTO dw.dim_department (department_name)
SELECT DISTINCT department_name
FROM dw.staging_operations
WHERE department_name NOT IN (
  SELECT department_name FROM dw.dim_department
);

-- Insert new locations into dim_location
INSERT INTO dw.dim_location (location_name)
SELECT DISTINCT location_name
FROM dw.staging_operations
WHERE location_name NOT IN (
  SELECT location_name FROM dw.dim_location
);

--  Add foreign keys to staging if not exists
DO $$ BEGIN
  ALTER TABLE dw.staging_operations ADD COLUMN process_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'process_id already exists';
END $$;

DO $$ BEGIN
  ALTER TABLE dw.staging_operations ADD COLUMN location_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'location_id already exists';
END $$;

DO $$ BEGIN
  ALTER TABLE dw.staging_operations ADD COLUMN department_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'department_id already exists';
END $$;

-- Map dim IDs to staging
UPDATE dw.staging_operations s
SET process_id = p.process_id
FROM dw.dim_process p
WHERE s.process_name = p.process_name;

UPDATE dw.staging_operations s
SET location_id = l.location_id
FROM dw.dim_location l
WHERE s.location_name = l.location_name;

UPDATE dw.staging_operations s
SET department_id = d.department_id
FROM dw.dim_department d
WHERE s.department_name = d.department_name;



--  Insert + audit for fact_downtime
WITH candidate_rows AS (
  SELECT
    s.department_id,
    s.process_id,
    s.location_id,
    t.time_id,
    s.downtime_hours
  FROM dw.staging_operations s
  JOIN dw.dim_time t
    ON t.full_date = s.process_date::DATE
),
inserted_rows AS (
  SELECT *
  FROM candidate_rows c
  WHERE NOT EXISTS (
    SELECT 1 FROM dw.fact_downtime f
    WHERE f.department_id = c.department_id
      AND f.process_id = c.process_id
      AND f.location_id = c.location_id
      AND f.time_id = c.time_id
      AND f.downtime_hours = c.downtime_hours
  )
),
do_insert AS (
  INSERT INTO dw.fact_downtime (
    department_id, process_id, location_id, time_id, downtime_hours
  )
  SELECT * FROM inserted_rows
  RETURNING *
)

-- Audit log
INSERT INTO dw.audit_log (
  job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
)
SELECT
  (SELECT job_id FROM temp_etl_job),
  'fact_downtime',
  'load_fact_downtime',
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