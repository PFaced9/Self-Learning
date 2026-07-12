from datetime import timedelta
from time import sleep
from airflow.sdk import dag, task
import random
import time

@dag(
    default_args={
        "retries":2,
        "retry_exponential_backoff": True,
        "retry_delay":timedelta(seconds= 1),
        "execution_timeout":timedelta(seconds= 5)
    },
    dag_id= "retry_mechanism",
)
def retry_testing_logic():
    @task.python
    def check_num():
        value = random.randint(-1,1)
        time.sleep(6)
        print(value)
        if value < 0:
         raise ValueError(f"no new rows to process")
        return True
    check_num()

retry_testing_logic()