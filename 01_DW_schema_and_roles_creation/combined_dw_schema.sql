-- Create schema
CREATE SCHEMA IF NOT EXISTS dw;


-- HR TABLES
-- Department Dimension
CREATE TABLE IF NOT EXISTS dw.dim_department (
  department_id SERIAL PRIMARY KEY,
  department_name VARCHAR(100) UNIQUE NOT NULL
);

-- Employee Dimension (SCD Type 2)
CREATE TABLE IF NOT EXISTS dw.dim_employee (
  employee_sk SERIAL PRIMARY KEY,
  employee_id TEXT NOT NULL,
  name VARCHAR(100),
  gender CHAR(1),
  date_of_joining DATE,
  manager_id TEXT,
  department_id INT REFERENCES dw.dim_department(department_id),
  row_hash TEXT,
  valid_from DATE,
  valid_to DATE,
  is_current BOOLEAN
);

-- dim_time
CREATE TABLE IF NOT EXISTS dw.dim_time (
  time_id SERIAL PRIMARY KEY,
  full_date DATE UNIQUE,
  day INT,
  month INT,
  quarter INT,
  year INT,
  is_weekend BOOLEAN
);

-- Populate dim_time for 2020–2030
INSERT INTO dw.dim_time (full_date, day, month, quarter, year, is_weekend)
SELECT
  date::DATE,
  EXTRACT(DAY FROM date)::INT,
  EXTRACT(MONTH FROM date)::INT,
  EXTRACT(QUARTER FROM date)::INT,
  EXTRACT(YEAR FROM date)::INT,
  CASE WHEN EXTRACT(DOW FROM date) IN (0,6) THEN TRUE ELSE FALSE END
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day') date
ON CONFLICT (full_date) DO NOTHING;

-- FallBack Date
INSERT INTO dw.dim_time (full_date, day, month, quarter, year, is_weekend)
SELECT
  '1957-01-01'::DATE,
  1, 1, 1, 1957,
  TRUE
WHERE NOT EXISTS (
  SELECT 1 FROM dw.dim_time WHERE full_date = '1957-01-01'
);

-- Employee Fact
CREATE TABLE IF NOT EXISTS dw.fact_employee (
  fact_id SERIAL PRIMARY KEY,
  employee_sk INT REFERENCES dw.dim_employee(employee_sk),
  time_id INT REFERENCES dw.dim_time(time_id),
  salary NUMERIC(12,2),
  status VARCHAR(20)
);


-- FINANCE TABLES


-- Expense Type Dimension
CREATE TABLE IF NOT EXISTS dw.dim_expense_type (
  expense_type_id SERIAL PRIMARY KEY,
  expense_type_name VARCHAR(100) UNIQUE NOT NULL
);

-- Finance Fact Table
CREATE TABLE IF NOT EXISTS dw.fact_expenses (
  fact_id SERIAL PRIMARY KEY,
  employee_sk INT REFERENCES dw.dim_employee(employee_sk),
  expense_type_id INT REFERENCES dw.dim_expense_type(expense_type_id),
  expense_amount NUMERIC(12,2),
  approved_by TEXT,
  time_id INT REFERENCES dw.dim_time(time_id),
  is_refund BOOLEAN
);


-- OPERATIONS TABLES


-- Process Dimension
CREATE TABLE IF NOT EXISTS dw.dim_process (
  process_id SERIAL PRIMARY KEY,
  process_name VARCHAR(100) UNIQUE NOT NULL
);

-- Location Dimension
CREATE TABLE IF NOT EXISTS dw.dim_location (
  location_id SERIAL PRIMARY KEY,
  location_name VARCHAR(100) UNIQUE NOT NULL
);

-- Downtime Fact
CREATE TABLE IF NOT EXISTS dw.fact_downtime (
  fact_id SERIAL PRIMARY KEY,
  department_id INT REFERENCES dw.dim_department(department_id),
  process_id INT REFERENCES dw.dim_process(process_id),
  location_id INT REFERENCES dw.dim_location(location_id),
  time_id INT REFERENCES dw.dim_time(time_id),
  downtime_hours NUMERIC(10,2)
);


-- LOGGING TABLES

-- Audit Log
CREATE TABLE IF NOT EXISTS dw.audit_log (
  log_id SERIAL PRIMARY KEY,
  job_id UUID DEFAULT gen_random_uuid(),
  table_name VARCHAR(100),
  etl_stage VARCHAR(100),
  rows_processed INT,
  rows_failed INT,
  log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status VARCHAR(20),
  message TEXT
);

-- Data Quality Log
CREATE TABLE IF NOT EXISTS dw.data_quality_log (
  dq_id SERIAL PRIMARY KEY,
  job_id UUID,
  table_name VARCHAR(100),
  column_name VARCHAR(100),
  row_reference TEXT,
  original_value TEXT,
  issue TEXT,
  log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- Staging Employee
DROP TABLE IF EXISTS dw.staging_employee;

CREATE TABLE dw.staging_employee (
  "EmployeeID" TEXT,
  "Name" TEXT,
  "Gender" TEXT,
  "DateOfJoining" TEXT,
  "ManagerID" TEXT,
  "Department" TEXT,
  "Salary" NUMERIC(12,2),
  "Status" TEXT
);


-- Staging table for Finance data
DROP TABLE IF EXISTS dw.staging_finance;

CREATE TABLE dw.staging_finance (
  employee_id TEXT,
  expense_type TEXT,
  expense_amount NUMERIC(12, 2),
  expense_date TEXT,
  approved_by TEXT,
  is_refund BOOLEAN
);


-- Staging Table for Operations
DROP TABLE IF EXISTS dw.staging_operations;

CREATE TABLE dw.staging_operations (
  department_name TEXT,
  process_name TEXT,
  location_name TEXT,
  process_date TEXT,
  downtime_hours TEXT
);
