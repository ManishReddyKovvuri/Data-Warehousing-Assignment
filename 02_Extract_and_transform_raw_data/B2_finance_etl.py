# 02_finance_etl_pipeline.py
import pandas as pd
import numpy as np
import uuid
from datetime import datetime
from sqlalchemy import create_engine

def finance_etl_pipeline():
    engine = create_engine("postgresql+psycopg2://postgres:root@localhost:5432/DW_DB")
    job_id = str(uuid.uuid4())

    finance_df = pd.read_excel("Finance_Dataset_Dirty.xlsx").copy()
    dq_log = []
    finance_df['row_number'] = finance_df.index + 1
    original_row_count = len(finance_df) 
    # Clean expense type
    finance_df['expense_type'] = finance_df['ExpenseType'].astype(str).str.strip().str.title()
    finance_df['expense_type'] = finance_df['expense_type'].replace({'Travell': 'Travel'})
    for i, val in enumerate(finance_df['expense_type']):
        if val.strip() == '' or pd.isna(val):
            dq_log.append({'job_id': job_id, 'table_name': 'staging_finance', 'column_name': 'expense_type',
                        'row_reference': finance_df.at[i, 'EmployeeID'], 'original_value': val,
                        'issue': 'Missing or empty expense type'})

    # Handle amounts
    finance_df['expense_amount'] = pd.to_numeric(finance_df['ExpenseAmount'], errors='coerce')
    finance_df['is_refund'] = finance_df['expense_amount'] < 0
    for i, val in enumerate(finance_df['expense_amount']):
        if pd.isnull(val):
            dq_log.append({'job_id': job_id, 'table_name': 'staging_finance', 'column_name': 'expense_amount',
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
                dq_log.append({'job_id': job_id, 'table_name': 'staging_finance', 'column_name': 'expense_date',
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
        'table_name': 'staging_finance',
        'etl_stage': 'finance_staging_load',
        'rows_processed': rows_processed,
        'rows_failed': rows_failed,
        'status': status,
        'message': f'Finance data cleaned. Processed: {rows_processed}, Failed: {rows_failed}, DQ Issues: {len(dq_log)}'
    }])
    audit_log.to_sql('audit_log', engine, schema='dw', if_exists='append', index=False)


    return(f"Finance ETL completed-Job ID:{job_id} ")


if __name__ == "__main__":
    print(finance_etl_pipeline())
