from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.teradata.transfers.postgres_to_teradata import PostgresToTeradataOperator

from datetime import datetime

# Airflow DAG 정의
dag = DAG(
    'postgres_to_teradata_pipeline',
    start_date=datetime(2024, 7, 16),
    schedule_interval=None  # 이 DAG는 수동으로 실행할 것이므로 스케줄은 None으로 설정
)

# PostgreSQL에서 Teradata로 데이터 이관하는 Task 정의
transfer_data_task = PostgresToTeradataOperator(
    task_id='transfer_data',
    sql='SELECT * FROM articles',  # PostgreSQL에서 데이터를 읽어올 SQL 쿼리
    teradata_table='articles',  # Teradata에 데이터를 쓸 테이블 이름
    teradata_conn_id='teradata_default',  # Teradata 연결을 위한 Connection ID
    postgres_conn_id='postgres_default',  # PostgreSQL 연결을 위한 Connection ID
    dag=dag
)

# PostgreSQL의 articles 테이블 삭제하는 Task 정의
delete_postgres_task = PostgresOperator(
    task_id='delete_postgres_table',
    sql='DELETE FROM articles',  # PostgreSQL에서 articles 테이블 삭제하는 SQL 쿼리
    postgres_conn_id='postgres_default',  # PostgreSQL 연결을 위한 Connection ID
    dag=dag
)

# Task 간의 실행 순서 설정 (의존성 설정)
transfer_data_task >> delete_postgres_task
