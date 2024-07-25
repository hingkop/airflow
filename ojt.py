from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.teradata.hooks.teradata import TeradataHook
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import teradatasql

# 한국 시간대 설정
kst = pendulum.timezone("Asia/Seoul")

# 기본 인자 설정
default_args = {
    "owner": "eunjung",
    "start_date": datetime(2024, 7, 22, tzinfo=kst),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

# DAG 정의
dag = DAG(
    dag_id='sj_ojt1',
    default_args=default_args,
    description='Extract data from PostgreSQL and store data to Teradata',
    schedule_interval="0 5 * * *",
    catchup=False,
)

def extract_data_from_postgres():
    # PostgreSQL 연결
    postgres_hook = PostgresHook(postgres_conn_id='postgresql_public')
    # 쿼리 실행
    sql_query = "SELECT * FROM public.articles"
    df = postgres_hook.get_pandas_df(sql_query)
    # CSV 파일로 저장
    today = pendulum.now(kst).strftime("%Y%m%d")
    csv_path = f"/home/eunjung/data/articles-postgres-{today}.csv"
    df.to_csv(csv_path, index=False, encoding='utf-8')

extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data_from_postgres,
    dag=dag
)

def load_data_to_teradata():
    # CSV 파일 경로
    today = pendulum.now(kst).strftime("%Y%m%d")
    csv_path = f"/home/eunjung/data/articles-postgres-{today}.csv"
    # CSV 파일을 DataFrame으로 읽기
    df = pd.read_csv(csv_path, encoding='utf-8')

    # Teradata 연결
    teradata_hook = TeradataHook(teradata_conn_id='teradata_conn')
    td_conn = teradata_hook.get_conn()
    td_cursor = td_conn.cursor()

    # 데이터 삽입 쿼리
    insert_query = """
    INSERT INTO QG_DB.articles (id, newstitle, href, datetime, content, press, journalist)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        for index, row in df.iterrows():
            td_cursor.execute(insert_query, tuple(row))
        td_conn.commit()
    except Exception as e:
        print(f"Error inserting data into Teradata: {e}")
        td_conn.rollback()
    finally:
        td_cursor.close()
        td_conn.close()

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_teradata,
    dag=dag,
)

# Task 순서 정의
extract_task >> load_task
