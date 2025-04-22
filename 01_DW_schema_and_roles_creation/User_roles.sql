-- Create roles

CREATE ROLE hr_user LOGIN PASSWORD 'hr_pass';
CREATE ROLE finance_user LOGIN PASSWORD 'fin_pass';
CREATE ROLE super_user LOGIN PASSWORD 'super_pass';
-- CREATE ROLE staging_executor LOGIN PASSWORD 'stage_pass';


GRANT CONNECT ON DATABASE "ETL_DB" TO hr_user;
GRANT CONNECT ON DATABASE "ETL_DB" TO finance_user;
GRANT CONNECT ON DATABASE "ETL_DB" TO super_user;
-- GRANT CONNECT, TEMP ON DATABASE "ETL_DB" TO staging_executor;


-- HR access
GRANT USAGE ON SCHEMA dw TO hr_user;
GRANT SELECT ON dw.dim_employee TO hr_user;
GRANT SELECT ON dw.fact_employee TO hr_user;
GRANT SELECT ON dw.dim_department TO hr_user;
GRANT SELECT ON dw.dim_time TO hr_user;

-- Finance access
GRANT USAGE ON SCHEMA dw TO finance_user;
GRANT SELECT ON dw.fact_expenses TO finance_user;
GRANT SELECT ON dw.dim_expense_type TO finance_user;
GRANT SELECT ON dw.dim_employee TO finance_user;
GRANT SELECT ON dw.dim_time TO finance_user;

-- Full read/write access
GRANT USAGE, CREATE ON SCHEMA dw TO super_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dw TO super_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dw TO super_user;




--Allow usage of the schema
-- GRANT USAGE,CREATE ON SCHEMA dw TO staging_executor;

-- -- Run as admin
-- ALTER TABLE dw.staging_employee OWNER TO staging_executor;
-- ALTER TABLE dw.staging_finance OWNER TO staging_executor;
-- ALTER TABLE dw.staging_operations OWNER TO staging_executor;


-- --Allow R/W access to all staging tables
-- GRANT  SELECT, INSERT, UPDATE, DELETE ON dw.staging_employee TO staging_executor;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dw.staging_finance TO staging_executor;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON dw.staging_operations TO staging_executor;
-- GRANT SELECT,INSERT ON dw.data_quality_log TO staging_executor;
-- GRANT SELECT,INSERT ON dw.audit_log TO staging_executor;

-- -- Grant read-only access to dimension tables 
-- GRANT SELECT ON dw.dim_department TO staging_executor;
-- GRANT SELECT ON dw.dim_expense_type TO staging_executor;
-- GRANT SELECT ON dw.dim_time TO staging_executor;
-- GRANT SELECT ON dw.dim_location TO staging_executor;
-- GRANT SELECT ON dw.dim_process TO staging_executor;


