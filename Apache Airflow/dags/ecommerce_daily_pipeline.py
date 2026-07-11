from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
import pendulum

with DAG(
    dag_id = "ecommerce_daily_pipeline",
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay":timedelta(seconds=10)
    },
    description="first ever airflow dag code",
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Kolkata"),
) as dag:
    task_1 = BashOperator(
        task_id="check_source_availability",
        bash_command="echo 'checking source availability'"
    )
    task_2 = BashOperator(
        task_id="extract",
        bash_command="echo 'extract data from source'"
    )
    task_3 = BashOperator(
        task_id="validate_data",
        bash_command="echo 'validate data from extraction'"
    )
    task_4 = BashOperator(
        task_id="load_data",
        bash_command="echo 'load data to staging'"
    )
    task_5 = BashOperator(
        task_id="transform_data",
        bash_command="echo 'running transformation'"
    )
    task_6 = BashOperator(
        task_id="run_dq_checks",
        bash_command="echo 'running data quality checks'"
    )
    task_7 = BashOperator(
        task_id="mark_success",
        bash_command="echo 'everything is success'"
    )

    task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7