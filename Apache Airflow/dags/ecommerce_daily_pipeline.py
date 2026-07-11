from datetime import timedelta
from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="ecommerce_dag",
    default_args={
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    description="first ever airflow dag code",
    schedule="@daily",
    catchup=False,
    start_date=pendulum.datetime(2026, 7, 11, tz="Asia/Kolkata"),
)
def ecommerce_daily_pipeline():
    @task.bash
    def check_source_availability() -> str:
        # runs as an actual shell command, not Python
        return "echo 'checking source availability'"

    @task
    def extract() -> dict:
        # this return value becomes an XCom, passed to the next task as an arg
        return {"orders": 120, "source": "myntra"}

    @task
    def validate_data(extracted: dict) -> dict:
        extracted["validated"] = True
        return extracted

    @task
    def load_data(validated: dict) -> str:
        return f"loaded {validated['orders']} orders to staging"

    @task
    def transform_data(load_status: str) -> str:
        return f"transformed after: {load_status}"

    @task
    def run_dq_checks(transform_status: str) -> str:
        return f"dq checks passed for: {transform_status}"

    @task
    def mark_success(dq_status: str) -> str:
        return f"success: {dq_status}"

    source_check = check_source_availability()
    extracted = extract()
    validated = validate_data(extracted)
    loaded = load_data(validated)
    transformed = transform_data(loaded)
    dq_result = run_dq_checks(transformed)
    result = mark_success(dq_result)

    source_check >> extracted
    extracted >> validated
    validated >> loaded
    loaded >> transformed
    transformed >> dq_result
    dq_result >> result


ecommerce_daily_pipeline()