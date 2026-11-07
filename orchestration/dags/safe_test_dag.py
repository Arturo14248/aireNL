from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="safe_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["local-test"],
)
def safe_test_dag():
    @task
    def say_hello():
        print("Airflow DAG ran successfully")

    say_hello()


safe_test_dag()
