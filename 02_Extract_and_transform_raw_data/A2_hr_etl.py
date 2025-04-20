# hr_etl_pipeline.py
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from sqlalchemy import create_engine


def hr_etl_pipeline():  
    # PostgreSQL connection
    engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/DW_DB")
    job_id = str(uuid.uuid4())

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
            dq_log.append({'job_id': job_id, 'table_name': 'dim_employee', 'column_name': 'Gender',
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
                dq_log.append({'job_id': job_id, 'table_name': 'dim_employee', 'column_name': 'DateOfJoining',
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
            dq_log.append({'job_id': job_id, 'table_name': 'dim_employee', 'column_name': 'Salary',
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
            dq_log.append({'job_id': job_id, 'table_name': 'dim_employee', 'column_name': 'Name',
                        'row_reference': row['EmployeeID'], 'original_value': row['Name'],
                        'issue': f'Missing name, set to {fallback_name}'})
            hr_df.at[i, 'Name'] = fallback_name

    # EmployeeID
    for i, row in hr_df.iterrows():
        if pd.isna(row['EmployeeID']):
            fallback_id = f"TEMP_{i + 1}"
            dq_log.append({'job_id': job_id, 'table_name': 'dim_employee', 'column_name': 'EmployeeID',
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
        'table_name': 'staging_employee',
        'etl_stage': 'hr_staging_load',
        'rows_processed': rows_processed,
        'rows_failed': rows_failed,
        'status': status,
        'message': message
    }])

    audit_log.to_sql('audit_log', engine, schema='dw', if_exists='append', index=False)

    return( f"HR ETL completed-Job ID:{job_id}" )

if __name__ == "__main__":
    print(hr_etl_pipeline())