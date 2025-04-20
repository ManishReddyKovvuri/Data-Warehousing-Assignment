-- 03_load_dim_fact_operations.sql

-- Step 0: Declare consistent job_id
DO $$
BEGIN
  BEGIN
    PERFORM 1 FROM temp_etl_job LIMIT 1;
  EXCEPTION WHEN undefined_table THEN
    CREATE TEMP TABLE temp_etl_job (job_id UUID);
    INSERT INTO temp_etl_job VALUES (gen_random_uuid());
  END;
END $$;

-- Step 1: Insert new process names into dim_process
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

-- Step 2: Insert new locations into dim_location
INSERT INTO dw.dim_location (location_name)
SELECT DISTINCT location_name
FROM dw.staging_operations
WHERE location_name NOT IN (
  SELECT location_name FROM dw.dim_location
);

-- Step 3: Add foreign keys to staging if not exists
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

-- Step 4: Map dim IDs to staging
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

-- -- Step 5: Insert into fact_downtime
-- INSERT INTO dw.fact_downtime (
--   department_id, process_id, location_id, time_id, downtime_hours
-- )
-- SELECT
--   s.department_id,
--   s.process_id,
--   s.location_id,
--   t.time_id,
--   s.downtime_hours
-- FROM dw.staging_operations s
-- JOIN dw.dim_time t
--   ON t.full_date = s.process_date::DATE
-- WHERE NOT EXISTS (
--   SELECT 1 FROM dw.fact_downtime f
--   WHERE f.department_id = s.department_id
--     AND f.process_id = s.process_id
--     AND f.location_id = s.location_id
--     AND f.time_id = t.time_id
--     AND f.downtime_hours = s.downtime_hours
-- );

-- -- Step 6: Audit log for fact_downtime insert
-- WITH inserted_rows AS (
--   SELECT COUNT(*) AS count
--   FROM dw.fact_downtime
--   WHERE time_id = (SELECT time_id FROM dw.dim_time WHERE full_date = CURRENT_DATE)
-- )
-- INSERT INTO dw.audit_log (
--   job_id, table_name, etl_stage, rows_processed, rows_failed, status, message
-- )
-- SELECT
--   (SELECT job_id FROM temp_etl_job),
--   'fact_downtime',
--   'load_fact_downtime',
--   r.count,
--   CASE WHEN r.count = 0 THEN 1 ELSE 0 END,
--   CASE WHEN r.count = 0 THEN 'partial' ELSE 'success' END,
--   'Inserted valid records into fact_downtime'
-- FROM inserted_rows r;




-- Step 5–6: Insert + audit for fact_downtime
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