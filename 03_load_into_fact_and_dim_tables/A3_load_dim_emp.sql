
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
FROM stg.staging_employee
WHERE UPPER(TRIM("Department")) NOT IN (
  SELECT UPPER(TRIM(department_name)) FROM dw.dim_department
);

--  Add row_hash to staging if not exists
DO $$ BEGIN
    ALTER TABLE stg.staging_employee ADD COLUMN row_hash TEXT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'column row_hash already exists, skipping';
END $$;

UPDATE stg.staging_employee
SET row_hash = md5(concat_ws('::', "Name", "Gender", "DateOfJoining", "ManagerID", "Department", "Salary", "Status"));

--  Map department_id from dim_department into staging_employee
DO $$ BEGIN
    ALTER TABLE stg.staging_employee ADD COLUMN department_id INT;
EXCEPTION WHEN duplicate_column THEN RAISE NOTICE 'department_id already exists, skipping';
END $$;

UPDATE stg.staging_employee s
SET department_id = d.department_id
FROM dw.dim_department d
WHERE UPPER(s."Department") = UPPER(d.department_name);

--  Expire existing dim_employee rows that have changed
UPDATE dw.dim_employee d
SET valid_to = CURRENT_DATE,
    is_current = FALSE
FROM stg.staging_employee s
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
FROM stg.staging_employee s
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
  e.employee_sk, t.time_id, s."Salary", s."Status"
FROM dw.dim_employee e
JOIN stg.staging_employee s
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
