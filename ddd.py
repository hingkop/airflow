from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.teradata.operators.teradata import TeradataOperator
from airflow.providers_manager import ProvidersManager
from datetime import timedelta, datetime
import pendulum

kst = pendulum.timezone("Asia/Seoul")
start_date = datetime.now(kst) - timedelta(days=1)

default_args = {
    "owner": "eunjung",
    "start_date": start_date,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

dag = DAG(
    dag_id="mssql_to_teradata_ver2",
    description="move data from mssql to teradata with teradata operator",
    schedule_interval="35 10 * * *",
    catchup=False,
    default_args=default_args
)


move_task = TeradataOperator(
    task_id="move_task",
    sql="INSERT INTO QG_DB.articles SELECT * FROM dbo.articles@mssql",
    teradata_conn_id="teradata_conn",
    dag=dag
)

move_task