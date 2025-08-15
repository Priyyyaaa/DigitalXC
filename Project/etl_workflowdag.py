from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import subprocess
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 8, 8 , 8), 
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    }

def load_csv_to_postgres():
   df = pd.read_csv('/opt/airflow/data/Data.csv')
   column_mapping = {
        'inc_number': 'Ticket_ID',
        'inc_category': 'Category',
        'inc_business_service': 'Sub_Category',
        'inc_priority': 'Priority',
        'inc_sys_created_on': 'Created_Date',
        'inc_resolved_at': 'Resolved_Date',
        'inc_state': 'Status',
        'inc_assignment_group': 'Assigned_Group',
        'inc_assigned_to': 'Technician',
        'inc_caller_id': 'Customer_Impact'
    }
   df = df.rename(columns=column_mapping)
   
   df['Created_Date'] = df['Created_Date'].str.replace('  ', ' ').str.strip()
   df['Created_Date'] = pd.to_datetime(df['Created_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

   df['Resolved_Date'] = df['Resolved_Date'].str.replace('  ', ' ').str.strip()
   df['Resolved_Date'] = pd.to_datetime(df['Resolved_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

   df['Created_Date'] = df['Created_Date'].replace({np.nan: None})
   df['Resolved_Date'] = df['Resolved_Date'].replace({np.nan: None})
   df['Resolution_Time_Hrs'] = np.where(
    df['Created_Date'].notna() & df['Resolved_Date'].notna(),
    (df['Resolved_Date'] - df['Created_Date']), np.nan ) # Keep as np.nan if either date is missing)   
   
   rows = [
            (
                r['Ticket_ID'], r['Category'], r['Sub_Category'],
                r['Priority'], r['Created_Date'], r['Resolved_Date'],
                r['Status'], r['Assigned_Group'], r['Technician'],
                r['Customer_Impact'], r['Resolution_Time_Hrs']
            )
            for _, r in df.iterrows()
        ]
   hook = PostgresHook(postgres_conn_id='itsm_servicenow')
   hook.insert_rows(
        table="servicenow",
        rows=rows,
        target_fields=[
                'Ticket_ID', 'Category', 'Sub_Category', 'Priority',
                'Created_Date', 'Resolved_Date', 'Status', 'Assigned_Group',
                'Technician', 'Customer_Impact', 'Resolution_Time_Hrs'
            ]
        )
def validate_dbt_models():
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='itsm_servicenow')
    
    # List of tables to validate
    tables_to_validate = [
        'servicenow_v1',
        'averageresolution_v1',
        'monthlyticketsummary_v1',
        'ticketclosurerate_v1'
    ]
    
    # Validate each table
    for table in tables_to_validate:
        result = pg_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        count = result[0][0]
        print(f"Table {table} has {count} rows")
        
        if count == 0:
            raise ValueError(f"Table {table} is empty")
    
    print("All DBT models validated successfully")
   
with DAG(
    dag_id='serviceticket_etl_workflow',
    default_args=default_args,
    description='A simple ETL workflow DAG',
    schedule=timedelta(days=1),
    tags=['itsm'],
    catchup=False,
) as dag:
    
    generate_queries = PythonOperator(
    task_id='insert_data_to_postgres',
    python_callable=load_csv_to_postgres
    )
    run_dbt_model_with_params = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/imp  && dbt run --profiles-dir /home/airflow/.dbt',

    #bash_command='cd /opt/airflow/models && dbt run',
    dag=dag
    )
    validate_models = PythonOperator(
        task_id='validate_dbt_models',
        python_callable=validate_dbt_models,
    )

    generate_queries >> run_dbt_model_with_params >> validate_models