from datetime import timedelta
from airflow.sdk import dag, task
import pendulum
import random


@dag(
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    dag_id="ecommerce_dag",
    description="first ever airflow dag code",
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Kolkata"),
)
def ecommerce_daily_pipeline():
    @task.python
    def check_source_availability() -> dict:
        print("checking the source data if connection is established")
        source_count = random.randint(-1,5)
        if source_count > 0:
            return {"status": True, "count":source_count}
        return {"status": False, "count":source_count}

    @task.python
    def extract(check_result: dict) -> int:
        if not check_result["status"]:
            raise ValueError("Unable to connect to the data base")
        else:
            db_count = check_result["count"]
            print(f"extracting {db_count} rows from the source table")
        return db_count

    @task.python
    def validate_data(extracted: int) -> int:
        if extracted <= 0:
            raise ValueError(f"data is not valid, extracted rows are:{extracted}")   
        return extracted

    @task
    def load_data(validated: int) -> dict:
        return {"orders": validated, "status": "staged"}

    @task
    def transform_data(loaded: dict) -> dict:
        loaded["transformed"] = True
        return loaded

    @task
    def run_dq_checks(transformed: dict) -> str:
        if transformed.get("orders", 0) <= 0:
            raise ValueError("Orders are 0")
        return "dq_passed"

    @task
    def mark_success(dq_status: str) -> str:
        return "success"

    source_check = check_source_availability()
    extracted = extract(source_check)
    validated = validate_data(extracted)
    loaded = load_data(validated)
    transformed = transform_data(loaded)
    dq_result = run_dq_checks(transformed)
    mark_success(dq_result)


ecommerce_daily_pipeline()