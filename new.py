from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.dbapi_hook import DbApiHook
from datetime import timedelta, datetime
import pendulum
import teradatasql

# Custom Teradata Hook
class TeradataHook(DbApiHook):
    conn_name_attr = 'teradata_conn_id'
    default_conn_name = 'teradata_default'
    supports_autocommit = True

    def __init__(self, teradata_conn_id='teradata_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.teradata_conn_id = teradata_conn_id

    def get_conn(self):
        conn = self.get_connection(self.teradata_conn_id)
        conn = teradatasql.connect(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            database=conn.schema
        )
        return conn

    def get_cursor(self):
        return self.get_conn().cursor()

# Custom Teradata Operator
class TeradataOperator(BaseOperator):
    template_fields = ('sql',)

    @apply_defaults
    def __init__(self, sql, teradata_conn_id='teradata_default', *args, **kwargs):
        super(TeradataOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.teradata_conn_id = teradata_conn_id

    def execute(self, context):
        hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(self.sql)
        except Exception as e:
            self.log.error(f"Error executing SQL: {self.sql}")
            self.log.error(f"Exception: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

# DAG 설정

# pendulum을 사용하여 Asia/Seoul 시간대 생성
kst = pendulum.timezone("Asia/Seoul")

# start_date에 timezone 설정
start_date = datetime.now(kst) - timedelta(days=1)

default_args = {
    "owner": "eunjung",
    "start_date": start_date,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # 재시도 횟수를 0으로 설정하여 실패 시 재시도하지 않도록 함
    "retry_delay": timedelta(minutes=3)  # 이 설정은 이제 의미가 없음
}

# DAG 설정
dag = DAG(
    dag_id="ojt",
    description="Move data from MSSQL to Teradata using a querygrid insert query",
    schedule_interval="55 8 * * *",
    catchup=False,
    default_args=default_args
)



move_task = TeradataOperator(
    task_id="move_mssql_to_teradata",
    sql="INSERT INTO QG_DB.articles SELECT * FROM dbo.articles@mssql",
    teradata_conn_id="teradata_conn",
    dag=dag
)

move_task

