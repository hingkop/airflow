def delete_postgres_data():
    pg_hook = PostgresHook(postgres_conn_id="postgresql_public")
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    try:
        pg_cursor.execute("DELETE FROM articles")
        pg_conn.commit()
    except Exception as e:
        print(f"Error deleting data from PostgreSQL: {e}")
        pg_conn.rollback()
    finally:
        pg_conn.close()


delete_task = PythonOperator(
    task_id="delete_task",
    python_callable=delete_postgres_data,
    dag=dag,
)