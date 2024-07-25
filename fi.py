from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.teradata.hooks.teradata import TeradataHook
from datetime import timedelta, datetime
import pendulum

# 한국 시간대 설정
kst = pendulum.timezone("Asia/Seoul")

# 기본 인자 설정
default_args = {
    "owner": "eunjung",
    "start_date": datetime(2024, 7, 22, tzinfo=kst),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# DAG 정의
dag = DAG(
    dag_id="ojt_final",
    description="Move data from MS SQL Server to Teradata",
    schedule_interval="05 08 * * *",
    catchup=False,
    default_args=default_args
)

def transfer_data():
    # MS SQL Server Hook으로 데이터 가져오기
    mssql_hook = MsSqlHook(mssql_conn_id="mssql_conn")
    td_hook = TeradataHook(teradata_conn_id="teradata_conn")
    
    # MS SQL Server에서 데이터 가져오기
    mssql_conn = mssql_hook.get_conn()
    mssql_cursor = mssql_conn.cursor()
    mssql_cursor.execute("SELECT id, newstitle, href, datetime, content, press, journalist FROM dbo.articles")
    rows = mssql_cursor.fetchall()
    
    # Teradata에 데이터 삽입하기
    td_conn = td_hook.get_conn()
    td_cursor = td_conn.cursor()
    
    insert_query = """
    INSERT INTO QG_DB.articles (id, newstitle, href, datetime, content, press, journalist)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        for row in rows:
            try:
                td_cursor.execute(insert_query, row)
            except Exception as e:
                print(f"Error processing row {row}: {e}")
        td_conn.commit()
    except Exception as e:
        print(f"Error inserting data into Teradata: {e}")
        td_conn.rollback()
    finally:
        td_cursor.close()
        td_conn.close()

    # MS SQL Server 연결 종료
    mssql_cursor.close()
    mssql_conn.close()

transfer_task = PythonOperator(
    task_id="transfer_task",
    python_callable=transfer_data,
    dag=dag,
)

transfer_task
