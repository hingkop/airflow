from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
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
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

# DAG 정의
dag = DAG(
    dag_id="ojt2",
    description="Move data from Oracle to Teradata",
    schedule_interval="05 08 * * *",
    catchup=False,
    default_args=default_args
)

def format_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return dt_str

def clean_and_transform_row(row):
    return (
        row[0],  # id
        row[1],  # title
        row[2],  # href
        format_datetime(row[3]),  # datetime
        row[4],  # content
        row[5],  # press
        row[6]   # journalist
    )

def transfer_data():
    # Oracle Hook으로 데이터 가져오기
    oracle_hook = OracleHook(oracle_conn_id="oracle_conn")
    td_hook = TeradataHook(teradata_conn_id="teradata_conn")
    
    # Oracle에서 데이터 가져오기
    oracle_conn = oracle_hook.get_conn()
    oracle_cursor = oracle_conn.cursor()
    oracle_cursor.execute("SELECT id, newstitle, href, datetime, content, press, journalist FROM articles")
    rows = oracle_cursor.fetchall()
    
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
                cleaned_row = clean_and_transform_row(row)
                td_cursor.execute(insert_query, cleaned_row)
            except Exception as e:
                print(f"Error processing row {row}: {e}")
        td_conn.commit()
    except Exception as e:
        print(f"Error inserting data into Teradata: {e}")
        td_conn.rollback()
    finally:
        td_cursor.close()
        td_conn.close()

    # Oracle 연결 종료
    oracle_cursor.close()
    oracle_conn.close()

transfer_task = PythonOperator(
    task_id="transfer_task",
    python_callable=transfer_data,
    dag=dag,
)

transfer_task
