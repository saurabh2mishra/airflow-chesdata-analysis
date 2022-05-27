from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from plugins.operators.filestosql_operator import WritePandasDfToSQL

def test_task_extract_partydata(test_dag, mocker):
    """
    Test to check task_extract_partydata task which uses a custom operator.
    """
    mock_hook = mocker.patch.object(
       SqliteHook,
       "get_conn",
       return_value=mocker.Mock()
    )

    task_write_df_to_sql = WritePandasDfToSQL(
            task_id="extract_partydata",
            file_path="tests/dags/test_data/test_party.csv",
            sql_conn_id="test",
            destination_table="party",
            database=mocker.Mock(),
            dag=test_dag
        )
    task_write_df_to_sql.execute(context=None)
    mock_hook.run.assert_called_once()
    mock_hook.insert_rows.assert_called_once()