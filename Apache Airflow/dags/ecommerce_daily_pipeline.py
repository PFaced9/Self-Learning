from datetime import timedelta
from airflow.sdk import dag, task
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
    def psql_db_conn() -> bool:
        print("checking the source data if connection is established")
        hook=PostgresHook(postgres_conn_id="app_postgres",)
        hook.get_first("SELECT 1")
        return True

    @task.python
    def check_watermark() -> str:
            print("Checking the latest watermark in the pipeline_watermark table")
            hook=PostgresHook(postgres_conn_id="app_postgres",)
            watermark_val= hook.get_first("SELECT last_extracted_at from pipeline_watermark WHERE pipeline_name = %s", parameters = ("ecommerce_dag",))
            return str(watermark_val[0])

    @task.python
    def extract(watermark: str) -> dict:
        hook= PostgresHook(postgres_conn_id="app_postgres")
        result= hook.get_first("SELECT count(*) AS cnt, MAX(updated_at) AS max_date FROM source_orders WHERE updated_at > %s", parameters= (watermark,))
        count, max_date= result 
        return {
            "count":count,
            "new_watermark":str(max_date),
            "old_watermark":watermark
        }
    @task.python
    def validate_data(extracted: dict) -> dict:
        if extracted["count"] <= 0:
            raise ValueError(f"no new rows to process, count={extracted['count']}")
        return extracted

    @task
    def load_data(validated: dict) -> dict:
        hook= PostgresHook(postgres_conn_id= "app_postgres")
        conn= hook.get_conn()
        cursor= conn.cursor()
        cursor.execute(
            "INSERT INTO public.staging_orders(order_id, customer_name, channel, amount, status, updated_at) \
                SELECT order_id, customer_name, channel, amount, status, updated_at FROM source_orders \
                    WHERE updated_at > %s AND updated_at <= %s \
                        ON CONFLICT (order_id) DO UPDATE SET \
                            customer_name = EXCLUDED.customer_name, \
                            channel = EXCLUDED.channel, \
                            amount = EXCLUDED.amount, \
                            status = EXCLUDED.status, \
                            updated_at = EXCLUDED.updated_at, \
                            loaded_at = now()", (validated["old_watermark"], validated["new_watermark"])
            )
        cursor.execute("UPDATE pipeline_watermark SET last_extracted_at = %s WHERE pipeline_name= %s", (validated["new_watermark"], "ecommerce_dag"))
        conn.commit()
        conn.close()
        return validated

    @task
    def transform_data(loaded: dict) -> dict:
        loaded["transformed"]= True
        return {"orders": loaded["count"], "status":"staged"}

    @task
    def run_dq_checks(transformed: dict) -> str:
        hook= PostgresHook(postgres_conn_id= "app_postgres")
        #check1 if the amount has any negative value.
        negative_val= hook.get_first("SELECT amount FROM staging_orders WHERE amount < 0")
        if (negative_val):
            raise ValueError("there are orders with amount < 0")
        #check2 if there is any status which is null.
        null_check= hook.get_first("SELECT status FROM staging_orders WHERE status IS NULL OR status = ' '")
        if (null_check):
            raise ValueError("there are status which are null or empty")

        return "dq_check_passed"


    @task
    def mark_success(dq_status: str) -> str:
        return "success"

    watermark_check = check_watermark()
    psql_db_conn() >> watermark_check
    extract_dates= extract(watermark_check)
    validated = validate_data(extract_dates)
    loaded = load_data(validated)
    transformed = transform_data(loaded)
    dq_result = run_dq_checks(transformed)
    mark_success(dq_result)


ecommerce_daily_pipeline()