
--1. Headcount Over Time
CREATE OR REPLACE VIEW dw.vw_kpi_headcount AS
SELECT 
  t.year,
  t.month,
  COUNT(DISTINCT f.employee_sk) AS Active_Headcount
FROM dw.fact_employee f
JOIN dw.dim_time t ON f.time_id = t.time_id
WHERE f.status = 'Active'
GROUP BY t.year, t.month
ORDER BY t.year, t.month;


--2. Attrition Over Time
CREATE OR REPLACE VIEW dw.vw_kpi_resignations AS
SELECT 
  t.year,
  t.month,
  COUNT(*) AS resignations
FROM dw.fact_employee f
JOIN dw.dim_time t ON f.time_id = t.time_id
WHERE f.status = 'Resigned'
GROUP BY t.year, t.month
ORDER BY t.year, t.month;


--3. Average Salary by Gender
CREATE OR REPLACE VIEW dw.vw_kpi_avg_salary_by_gender AS
SELECT 
  e.gender,
  ROUND(AVG(f.salary), 2) AS avg_salary
FROM dw.fact_employee f
JOIN dw.dim_employee e ON f.employee_sk = e.employee_sk
WHERE e.is_current = TRUE
GROUP BY e.gender;



--4a Gross Monthly Expenses by Department and Expense type
CREATE OR REPLACE VIEW dw.vw_kpi_gross_monthly_expenses_by_dept AS
SELECT 
  t.year,
  t.month,
  d.department_name,
  ex.expense_type_name,
  ROUND(SUM(f.expense_amount), 2) AS total_expense
FROM dw.fact_expenses f
JOIN dw.dim_time t ON f.time_id = t.time_id
JOIN dw.dim_expense_type ex on f.expense_type_id = ex.expense_type_id
JOIN dw.dim_employee e ON f.employee_sk = e.employee_sk
JOIN dw.dim_department d ON e.department_id = d.department_id
WHERE f.is_refund =FALSE
GROUP BY t.year, t.month, d.department_name, ex.expense_type_name
ORDER BY t.year, t.month, d.department_name, ex.expense_type_name;


--4B NEt Monthly Expenses by Department and Expense type
CREATE OR REPLACE VIEW dw.vw_kpi_net_monthly_expenses_by_dept AS
SELECT 
  t.year,
  t.month,
  d.department_name,
  ex.expense_type_name,
  ROUND(SUM(f.expense_amount), 2) AS total_expense
FROM dw.fact_expenses f
JOIN dw.dim_time t ON f.time_id = t.time_id
JOIN dw.dim_expense_type ex on f.expense_type_id = ex.expense_type_id
JOIN dw.dim_employee e ON f.employee_sk = e.employee_sk
JOIN dw.dim_department d ON e.department_id = d.department_id
GROUP BY t.year, t.month, d.department_name, ex.expense_type_name
ORDER BY t.year, t.month, d.department_name,ex.expense_type_name;


--5 downtime by process
CREATE OR REPLACE VIEW dw.vw_kpi_downtime_by_process AS
SELECT 
  p.process_name,
  ROUND(SUM(f.downtime_hours), 2) AS total_downtime,
  ROUND(AVG(f.downtime_hours), 2) AS AVG_downtime
FROM dw.fact_downtime f
JOIN dw.dim_process p ON f.process_id = p.process_id
GROUP BY p.process_name
ORDER BY total_downtime DESC;

--6 Downtime by Deparment
CREATE OR REPLACE VIEW dw.vw_kpi_downtime_by_dept AS
SELECT 
  d.department_name,
  ROUND(SUM(f.downtime_hours), 2) AS total_downtime,
  ROUND(AVG(f.downtime_hours), 2) AS AVG_downtime
FROM dw.fact_downtime f
JOIN dw.dim_department d ON f.department_id = d.department_id
GROUP BY d.department_name
ORDER BY total_downtime DESC;



