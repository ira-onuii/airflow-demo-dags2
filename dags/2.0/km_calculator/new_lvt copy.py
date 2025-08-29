from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pendulum import timezone

today = datetime.now().strftime("%Y. %m. %d")

today2 = datetime.now().date()

print(today2)