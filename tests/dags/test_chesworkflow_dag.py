import glob
from os import path
import pytest
from datetime import datetime

from airflow import DAG
from airflow import models
from plugins.operators.filestosql_operator import WritePandasDfToSQL

DAG_PATHS = glob.glob(path.join(path.dirname(__file__), "..", "..", "dags", "*.py"))


def test_task_extract_partydata(test_dag, mocker):
    """
    Custom Operator Test
    """

    dag = DAG(dag_id="test", start_date=datetime.now())
    mock_hook = mocker.patch(
        "plugins.operators.filestosql_operator.SqliteHook", return_value=mocker.Mock()
    )

    task_write_df_to_sql = WritePandasDfToSQL(
        task_id="extract_partydata",
        file_path="tests/data/test_party.csv",
        sql_conn_id="test",
        destination_table="test",
        database="db.test",
        dag=dag,
    )
    ti = models.TaskInstance(
        task=task_write_df_to_sql, run_id="test", execution_date=datetime.now()
    )
    task_write_df_to_sql.execute(ti.get_template_context())
    mock_hook.run.assert_called()
    mock_hook.insert_rows.assert_called()
    # pytest.helpers.run_task(task=task_write_df_to_sql, dag=test_dag)


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path):
    """Import DAG files and check for a valid DAG instance."""
    dag_name = path.basename(dag_path)
    module = _import_file(dag_name, dag_path)
    # Validate if there is at least 1 DAG object in the file
    dag_objects = [var for var in vars(module).values() if isinstance(var, models.DAG)]
    assert dag_objects

    # For every DAG object, test for cycles
    for dag in dag_objects:
        dag.test_cycle()


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
