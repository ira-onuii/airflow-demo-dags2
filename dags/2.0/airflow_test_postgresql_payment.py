import sys
import os

# 현재 파일이 있는 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import warehouse_query2

from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
#from airflow.operators.bash import BashOperator
import pandas as pd
from io import StringIO
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y-%m-%d"))

payment_filename = 'pg_payment_'+date + '.csv'


#payment
def payment_save_to_s3_with_hook(data, bucket_name, folder_name, file_name):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    hook = S3Hook(aws_conn_id='conn_S3')
    hook.load_string(csv_buffer.getvalue(), key=f"{folder_name}/{file_name}", bucket_name=bucket_name, replace=True)


def payment_save_results_to_s3(**context):
    query_results = context['ti'].xcom_pull(task_ids='payment_run_select_query')
    column_names = ["payment_No", "user_No", "LGD_AMOUNT", "LGD_BUYER", "order_id", "LGD_TID", "LGD_PAYTYPE", "TTEOK_HAM_No", "LGD_PRODUCTINFO", "payment_regdate", "memo", "lecture_vt_No", "supply_value", "additional_tax", "cancelled_supply_value", "cancelled_additional_tax", "completed_at", "discountd_value", "cancelled_pg_tid", "cancelled_amount", "cancelled_pg_fee", "state", "original_payment_id", "payment_method"]
    df = pd.DataFrame(query_results, columns=column_names)
    payment_save_to_s3_with_hook(df, 'onuii-data-pipeline', 'payment', payment_filename)

def payment_insert_postgres_data(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    records = context['ti'].xcom_pull(task_ids='payment_run_select_query')
    insert_query = warehouse_query2.payment_insert_query

    pg_hook = PostgresHook(postgres_conn_id='postgres_dev_conn')
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()

    for record in records:
        pg_cursor.execute(insert_query, (
            record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21], record[22], record[23]
        ))

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-postgresql-payment_2.0',
    default_args=default_args,
    description='Run query and load result to S3',
    schedule='45 17 * * *',
    tags=["2.0"]
)




#payment
payment_run_query = SQLExecuteQueryOperator(
    task_id='payment_run_select_query',
    sql=warehouse_query2.payment_select_query,
    conn_id='legacy_staging_conn',
    do_xcom_push=True,
    dag=dag,
)

payment_delete_row = SQLExecuteQueryOperator(
    task_id="payment_delete_row",
    conn_id='postgres_dev_conn',
    sql=warehouse_query2.payment_delete_query
)

payment_insert_data = PythonOperator(
    task_id='insert_payment_data',
    python_callable=payment_insert_postgres_data,
    provide_context=True,
)

payment_save_to_s3_task = PythonOperator(
    task_id='payment_save_to_s3',
    python_callable=payment_save_results_to_s3,
    provide_context=True,
)





# dbt_run = BashOperator(
#     task_id='dbt_run',
#     bash_command='dbt run --profiles-dir /opt/airflow/dbt_project/.dbt --project-dir /opt/airflow/dbt_project --models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql',
# )

payment_run_query >> payment_delete_row >> payment_insert_data >> payment_save_to_s3_task 


