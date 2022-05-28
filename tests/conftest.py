import os
import datetime
import pytest

from airflow import DAG

pytest_plugins = ["helpers_namespace"]
os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"]="False"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"]="False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"]="True"
os.environ["AIRFLOW_SHOME"]=os.path.dirname(os.path.dirname(__file__))

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2022, 5, 24)},
        schedule_interval=datetime.timedelta(days=1),
    )


@pytest.helpers.register
def run_task(task, dag):
    "Run an Airflow task."
    dag.clear()
    task.run(start_date=dag.start_date, end_date=dag.start_date)


def reset_db():
    from airflow.utils import db
    db.resetdb()
    yield
    #cleanup
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.cfg"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.db"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "webserver_config.py"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "logs"))