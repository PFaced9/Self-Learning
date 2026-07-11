from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds = 5)
    },
    dag_id= "test_connection",
    catchup= False,
    schedule= "@daily",
    start_date= pendulum.datetime(2026, 7, 11, tz="Asia/Kolkata"),
)
def test_connection():
    @task.python
    def check_connection() -> str:
       hook= PostgresHook(postgres_conn_id="app_postgres")
       return hook.run("SELECT * FROM source_orders")
    check_connection()

test_connection()