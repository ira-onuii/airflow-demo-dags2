from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from io import StringIO
#import query


# date = str(((datetime.now()) + timedelta(hours=9)).strftime("%Y_%m_%d"))
# filename = 'pg_daily_active_lecture_list_'+date + '.csv'
# table_id = f'data-warehouse-test-428406.datawarehouse.{date}'



# def upload_to_s3(**kwargs):
#     pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     cursor.execute(f"SELECT * FROM pg_daily_active_lecture_list_{date}")
#     rows = cursor.fetchall()
#     columns = [desc[0] for desc in cursor.description]
#     df = pd.DataFrame(rows, columns=columns)
#     s3_hook = S3Hook(aws_conn_id='conn_S3')
#     buffer = StringIO()
#     df.to_csv(buffer, index=False)
#     s3_hook.load_string(
#         string_data=buffer.getvalue(),
#         key=f"daily_active_lecture/{filename}",
#         bucket_name='onuii-data-pipeline',
#         replace=True
#     )

# def new_lecture_insert_postgres_data(**context):
#     from airflow.providers.postgres.hooks.postgres import PostgresHook

#     records = context['ti'].xcom_pull(task_ids='new_lecture_run_query')
#     insert_query = query.new_insert_query

#     pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
#     pg_conn = pg_hook.get_conn()
#     pg_cursor = pg_conn.cursor()

#     for record in records:
#         pg_cursor.execute(insert_query, (
#             record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12]
#         ))

#     pg_conn.commit()
#     pg_cursor.close()
#     pg_conn.close()

# def pause_lecture_insert_postgres_data(**context):
#     from airflow.providers.postgres.hooks.postgres import PostgresHook

#     records = context['ti'].xcom_pull(task_ids='pause_lecture_run_query')
#     insert_query = query.pause_insert_query

#     pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
#     pg_conn = pg_hook.get_conn()
#     pg_cursor = pg_conn.cursor()

#     for record in records:
#         pg_cursor.execute(insert_query, (
#             record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21], record[22], record[23], record[24], record[25], record[26], record[27], record[28]
#         ))

#     pg_conn.commit()
#     pg_cursor.close()
#     pg_conn.close()



default_args = {
    'owner': 'Chad',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'data-warehouse-test-postgresql-dbt',
    default_args=default_args,
    description='Run Trino query and load result to S3',
    schedule='0 16 * * *',
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --model --select ./dbt/dbt_project/models/KPIs/company/student_indicators.sql',
    # dbt run --profiles-dir /opt/airflow/dbt/dbt_project/.dbt --project-dir /opt/airflow/dbt/dbt_project --model ./dbt/dbt_project/models/kpis/company/student_indicatrs
    #'dbt run --profiles-dir/opt/airflow/dags/repo/dbt/dbt_dev/.dbt --project-dir /opt/airflow/dags/repo/dbt/dbt_dev --models /opt/airflow/dags/repo/dbt/dbt_dev/models/KPIs/company/student_indicators.sql',
    dag=dag,
)

#--models /opt/airflow/dbt_project/models/pg_active_lecture/active_lecture.sql

# save_to_s3_task = PythonOperator(
#     task_id='save_to_s3',
#     python_callable=upload_to_s3,
#     provide_context=True,
#     dag=dag,
# )



dbt_run 
