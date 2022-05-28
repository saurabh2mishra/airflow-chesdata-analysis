import os
import datetime
import pytest

from airflow import DAG

pytest_plugins = ["helpers_namespace"]

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", 
        "start_date": datetime.datetime(2022, 5, 24)},
        schedule_interval=datetime.timedelta(days=1),
    )

@pytest.helpers.register
def run_task(task, dag):
    "Run an Airflow task."
    dag.clear()
    task.run(start_date=dag.start_date, end_date=dag.start_date)

def initial_db_init():
    if os.environ.get("RUN_AIRFLOW_1_10") == "true":
        print("Attempting to reset the db using airflow command")
        os.system("airflow resetdb -y")
    else:
        from airflow.utils import db
        
        db.resetdb()