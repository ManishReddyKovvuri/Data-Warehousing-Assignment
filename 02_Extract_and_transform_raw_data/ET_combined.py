

import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from sqlalchemy import create_engine


def hr_etl_pipeline(job_id=None, engine=None):  
    # PostgreSQL connection
    engine = engine or create_engine("postgresql+psycopg2://postgres:root@localhost:5432/ETL_DB")
    job_id = job_id or str(uuid.uuid4())

    # Load HR dataset
    hr_df = pd.read_excel("HR_Dataset_Dirty.xlsx").copy()
    dq_log = []
    original_row_count =len(hr_df)
    # Helper columns
    hr_df['row_number'] = hr_df.index + 1

    # Department
    hr_df['Department'] = hr_df['Department'].astype(str).str.strip().str.upper()
    hr_df['Department'] = hr_df['Department'].replace(['', 'NAN', 'NaN', 'nan','null'], 'UNASSIGNED_DEPT')
    hr_df['Department'] = hr_df['Department'].fillna('UNASSIGNED_DEPT')


    # Gender
    original_gender = hr_df['Gender'].astype(str).str.strip().str.upper()
    gender_map = {'m': 'M', 'MALE': 'M', 'f': 'F', 'FEMALE': 'F'}
    hr_df['Gender'] = original_gender.replace(gender_map)
    hr_df['Gender'] = hr_df['Gender'].apply(lambda x: x if x in ['M', 'F'] else 'UNKNOWN')

    for i, val in enumerate(original_gender):
        if val not in gender_map and val not in ['M', 'F']:
            dq_log.append({'job_id': job_id, 'table_name': 'raw_hr', 'column_name': 'Gender',
                        'row_reference': hr_df.at[i, 'EmployeeID'], 'original_value': val,
                        'issue': 'Unknown gender, set to UNKNOWN'})

    # DateOfJoining
    def fix_date(date_val, row_index):
        try:
            return pd.to_datetime(date_val).strftime("%Y-%m-%d")
        except:
            try:
                return pd.to_datetime(date_val, dayfirst=True).strftime("%Y-%m-%d")
            except:
                dq_log.append({'job_id': job_id, 'table_name': 'raw_hr', 'column_name': 'DateOfJoining',
                            'row_reference': hr_df.at[row_index, 'EmployeeID'], 'original_value': date_val,
                            'issue': 'Invalid date format'})
                return np.nan

    hr_df['DateOfJoining'] = [fix_date(val, i) for i, val in enumerate(hr_df['DateOfJoining'])]

    # ManagerID
    hr_df['ManagerID'] = hr_df['ManagerID'].astype(str).str.strip()

    # Salary
    hr_df['Salary'] = pd.to_numeric(hr_df['Salary'], errors='coerce')
    for i, val in enumerate(hr_df['Salary']):
        if pd.notnull(val) and val < 0:
            dq_log.append({'job_id': job_id, 'table_name': 'raw_hr', 'column_name': 'Salary',
                        'row_reference': hr_df.at[i, 'EmployeeID'], 'original_value': val,
                        'issue': 'Negative salary converted to positive'})
    hr_df['Salary'] = hr_df['Salary'].abs()

    # Status
    status_standard = {'ACTIVE': 'Active', 'RESIGNED': 'Resigned'}
    hr_df['Status'] = hr_df['Status'].astype(str).str.strip().str.upper().replace(status_standard)
    hr_df['Status'] = hr_df['Status'].apply(lambda x: x if x in ['Active', 'Resigned'] else 'Unknown')

    # Name
    for i, row in hr_df.iterrows():
        if pd.isna(row['Name']) or str(row['Name']).strip() == '':
            fallback_name = f"EMP_{row['EmployeeID']}" if pd.notna(row['EmployeeID']) else "Unknown Name"
            dq_log.append({'job_id': job_id, 'table_name': 'raw_hr', 'column_name': 'Name',
                        'row_reference': row['EmployeeID'], 'original_value': row['Name'],
                        'issue': f'Missing name, set to {fallback_name}'})
            hr_df.at[i, 'Name'] = fallback_name

    # EmployeeID
    for i, row in hr_df.iterrows():
        if pd.isna(row['EmployeeID']):
            fallback_id = f"TEMP_{i + 1}"
            dq_log.append({'job_id': job_id, 'table_name': 'raw_hr', 'column_name': 'EmployeeID',
                        'row_reference': 'Unknown', 'original_value': row['EmployeeID'],
                        'issue': f'Missing EmployeeID, set to {fallback_id}'})
            hr_df.at[i, 'EmployeeID'] = fallback_id

    # Drop helper column
    hr_df.drop(columns=['row_number'], inplace=True)

    # Remove  duplicates
    hr_df_cleaned = hr_df.drop_duplicates()

    # Save staging table 
    hr_df_cleaned.to_sql("staging_employee", engine, schema="dw", if_exists="replace", index=False)

    # Save DQ logs
    pd.DataFrame(dq_log).to_sql("data_quality_log", engine, schema="dw", if_exists="append", index=False)




    rows_processed = len(hr_df_cleaned)  
    rows_failed = original_row_count - rows_processed
    status = 'success' if rows_failed == 0 else 'partial'
    message = f"Processed: {rows_processed}, Failed: {rows_failed}, DQ Issues: {len(dq_log)}"

    # Build audit log record
    audit_log = pd.DataFrame([{
        'job_id': job_id,
        'table_name': 'raw_hr',
        'etl_stage': 'hr_staging_load',
        'rows_processed': rows_processed,
        'rows_failed': rows_failed,
        'status': status,
        'message': message
    }])

    audit_log.to_sql('audit_log', engine, schema='dw', if_exists='append', index=False)

    return( f"HR ETL completed-Job ID:{job_id}" )

def finance_etl_pipeline(job_id=None, engine=None):
    engine =engine or create_engine("postgresql+psycopg2://postgres:root@localhost:5432/ETL_DB")
    job_id = job_id or str(uuid.uuid4())

    finance_df = pd.read_excel("Finance_Dataset_Dirty.xlsx").copy()
    dq_log = []
    finance_df['row_number'] = finance_df.index + 1
    original_row_count = len(finance_df) 
    # Clean expense type
    finance_df['expense_type'] = finance_df['ExpenseType'].astype(str).str.strip().str.title()
    finance_df['expense_type'] = finance_df['expense_type'].replace({'Travell': 'Travel'})
    for i, val in enumerate(finance_df['expense_type']):
        if val.strip() == '' or pd.isna(val):
            dq_log.append({'job_id': job_id, 'table_name': 'raw_finance', 'column_name': 'expense_type',
                        'row_reference': finance_df.at[i, 'EmployeeID'], 'original_value': val,
                        'issue': 'Missing or empty expense type'})

    # Handle amounts
    finance_df['expense_amount'] = pd.to_numeric(finance_df['ExpenseAmount'], errors='coerce')
    finance_df['is_refund'] = finance_df['expense_amount'] < 0
    for i, val in enumerate(finance_df['expense_amount']):
        if pd.isnull(val):
            dq_log.append({'job_id': job_id, 'table_name': 'raw_finance', 'column_name': 'expense_amount',
                        'row_reference': finance_df.at[i, 'EmployeeID'], 'original_value': finance_df.at[i, 'ExpenseAmount'],
                        'issue': 'Invalid or missing expense amount'})
    # finance_df['expense_amount'] = finance_df['expense_amount'].abs()

    # Fix date
    def fix_date(date_val, row_index):
        try:
            return pd.to_datetime(date_val).strftime("%Y-%m-%d")
        except:
            try:
                return pd.to_datetime(date_val, dayfirst=True).strftime("%Y-%m-%d")
            except:
                dq_log.append({'job_id': job_id, 'table_name': 'raw_finance', 'column_name': 'expense_date',
                            'row_reference': finance_df.at[row_index, 'EmployeeID'], 'original_value': date_val,
                            'issue': 'Invalid date format'})
                return np.nan

    finance_df['expense_date'] = [fix_date(val, i) for i, val in enumerate(finance_df['ExpenseDate'])]

    #ApprovedBy
    finance_df['approved_by'] = finance_df['ApprovedBy'].apply(
        lambda x: str(int(x)) if pd.notna(x) and isinstance(x, float) and x.is_integer() else str(x)
    ).str.strip()
    finance_df['approved_by'] = finance_df['approved_by'].replace(['nan', 'NaN', '', 'None'], 'UNKNOWN')
    for i, val in enumerate(finance_df['approved_by']):
        if val == 'UNKNOWN':
            dq_log.append({
                'job_id': job_id,
                'table_name': 'staging_finance',
                'column_name': 'approved_by',
                'row_reference': finance_df.at[i, 'EmployeeID'],
                'original_value': finance_df.at[i, 'ApprovedBy'],
                'issue': 'Missing or invalid approved_by, set to NULL'
            })


    # Prepare final staging columns
    finance_df['employee_id'] = finance_df['EmployeeID'].astype(str).str.strip()
    finance_df = finance_df[['employee_id', 'expense_type', 'expense_amount', 'expense_date', 'approved_by', 'is_refund']]
    finance_df.drop_duplicates(inplace=True)

    # Load to staging
    finance_df.to_sql("staging_finance", engine, schema="dw", if_exists="replace", index=False)

    # Log DQ
    pd.DataFrame(dq_log).to_sql("data_quality_log", engine, schema="dw", if_exists="append", index=False)


    rows_processed = len(finance_df)
    rows_failed = original_row_count - rows_processed
    status = 'success' if rows_failed == 0 else 'partial'


    audit_log = pd.DataFrame([{
        'job_id': job_id,
        'table_name': 'raw_finance',
        'etl_stage': 'finance_staging_load',
        'rows_processed': rows_processed,
        'rows_failed': rows_failed,
        'status': status,
        'message': f'Finance data cleaned. Processed: {rows_processed}, Failed: {rows_failed}, DQ Issues: {len(dq_log)}'
    }])
    audit_log.to_sql('audit_log', engine, schema='dw', if_exists='append', index=False)


    return(f"Finance ETL completed-Job ID:{job_id} ")


def operations_etl_pipeline(job_id=None, engine=None):
    engine = engine or create_engine("postgresql+psycopg2://postgres:root@localhost:5432/ETL_DB")
    job_id = job_id or str(uuid.uuid4())

    ops_df = pd.read_excel("Operations_Dataset_Dirty.xlsx").copy()
    dq_log = []
    original_row_count =len(ops_df)

    # Clean Department
    ops_df['department_name'] = ops_df['Department'].astype(str).str.strip().str.upper()
    ops_df['department_name'] = ops_df['department_name'].replace(
        ['', 'NAN', 'NaN', 'nan'], 'UNASSIGNED_DEPT'
    ).fillna('UNASSIGNED_DEPT')
    ops_df['department_name'] = ops_df['department_name'].fillna('UNASSIGNED_DEPT')

    for i, val in enumerate(ops_df['department_name']):
        if val=='UNASSIGNED_DEPT':
            dq_log.append({
                'job_id': job_id, 'table_name': 'raw_operations', 'column_name': 'department_name',
                'row_reference': str(i + 1), 'original_value': ops_df.at[i, 'department_name'],
                'issue': 'Department Name is empty, defaulted to UNASSIGNED_DEPT'
            })




    # Clean Process
    ops_df['process_name'] = ops_df['ProcessName'].astype(str).str.strip().str.upper()
    ops_df['process_name'] = ops_df['process_name'].replace(
        ['', 'NAN', 'NaN'], 'UNKNOWN_PROCESS'
    )
    for i, val in enumerate(ops_df['process_name']):
        if val=='UNKNOWN_PROCESS':
            dq_log.append({
                'job_id': job_id, 'table_name': 'raw_operations', 'column_name': 'process_name',
                'row_reference': str(i + 1), 'original_value': ops_df.at[i, 'process_name'],
                'issue': 'Process Name is empty, defaulted to UNKNOWN_PROCESS'
            })

    # Clean Location
    ops_df['location_name'] = ops_df['Location'].astype(str).str.strip().str.upper()
    ops_df['location_name'] = ops_df['location_name'].replace(
        ['', 'NAN', 'NaN'], 'UNKNOWN_LOCATION'
    )
    for i, val in enumerate(ops_df['location_name']):
        if val=='UNKNOWN_LOCATION':
            dq_log.append({
                'job_id': job_id, 'table_name': 'raw_operations', 'column_name': 'location_name',
                'row_reference': str(i + 1), 'original_value': ops_df.at[i, 'location_name'],
                'issue': 'Location Name is empty, defaulted to UNKNOWN_LOCATION'
            })

    # Clean downtime_hours
    ops_df['downtime_hours'] = pd.to_numeric(ops_df['DowntimeHours'], errors='coerce')
    # Compute group averages where downtime is available
    group_avg = (
        ops_df.dropna(subset=['downtime_hours'])
        .groupby(['department_name', 'process_name', 'location_name'])['downtime_hours']
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={'downtime_hours': 'avg_downtime_hours'})
    )

    # Merge and fill missing downtime_hours using group averages
    ops_df = ops_df.merge(group_avg, on=['department_name', 'process_name', 'location_name'], how='left')
    ops_df['downtime_hours'] = ops_df['downtime_hours'].fillna(ops_df['avg_downtime_hours'])
    # Log unfixable downtime values
    for i, val in enumerate(ops_df['downtime_hours']):
        if pd.isnull(val):
            dq_log.append({
                'job_id': job_id, 'table_name': 'raw_operations', 'column_name': 'downtime_hours',
                'row_reference': str(i + 1), 'original_value': ops_df.at[i, 'DowntimeHours'],
                'issue': 'Downtime missing and no group average available'
            })

    # Fallback for unfixable downtime (if any)
    ops_df['downtime_hours'] = ops_df['downtime_hours'].fillna(0)
    # Clean process_date
    def fix_date(date_val, row_index):
        try:
            return pd.to_datetime(date_val).strftime("%Y-%m-%d")
        except:
            try:
                return pd.to_datetime(date_val, dayfirst=True).strftime("%Y-%m-%d")
            except:
                dq_log.append({
                    'job_id': job_id, 'table_name': 'raw_operations',
                    'column_name': 'process_date',
                    'row_reference': str(row_index + 1),  
                    'original_value': date_val,
                    'issue': 'Invalid date format, set to 1957-01-01'
                })
                return '1957-01-01'

    ops_df['process_date'] = [fix_date(val, i) for i, val in enumerate(ops_df['ProcessDate'])]

    # Final cleanup
    ops_df = ops_df[[
        'department_name', 'process_name', 'location_name',
        'process_date', 'downtime_hours'
    ]]
    ops_df.drop_duplicates(inplace=True)

    # Load to staging
    ops_df.to_sql("staging_operations", engine, schema="dw", if_exists="replace", index=False)

    # Save DQ log
    if dq_log:
        pd.DataFrame(dq_log).to_sql("data_quality_log", engine, schema="dw", if_exists="append", index=False)

    # Audit log


    rows_processed = len(ops_df)  


    rows_failed = original_row_count - rows_processed
    status = 'success' if rows_failed == 0 else 'partial'
    message = f"Processed: {rows_processed}, Failed: {rows_failed}, DQ Issues: {len(dq_log)}"

    audit_log = pd.DataFrame([{
        'job_id': job_id,
        'table_name': 'raw_operations',
        'etl_stage': 'operations_staging_load',
        'rows_processed': rows_processed,
        'rows_failed': rows_failed,
        'status': status,
        'message': message
    }])
    audit_log.to_sql("audit_log", engine, schema="dw", if_exists="append", index=False)

    return(f"Operations ETL completed- Job ID:{job_id}")






job_id = str(uuid.uuid4())  
engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/ETL_DB")
print(hr_etl_pipeline(job_id,engine))
print(finance_etl_pipeline(job_id,engine))
print(operations_etl_pipeline(job_id,engine))