# 02_operations_etl_pipeline.py
import pandas as pd
import numpy as np
import uuid
from sqlalchemy import create_engine


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


if __name__ == "__main__":
    print(operations_etl_pipeline())