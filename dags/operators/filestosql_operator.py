import pandas as pd

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class WritePandasDfToSQL(BaseOperator):
    """ "
    A custom operator to read csv files and put df into sqlite db.
    :param Path file_path: file or uri
    :param String destination_table: destination_table
    """

    ui_color = "#33DDFF"

    @apply_defaults
    def __init__(
        self, file_path, sql_conn_id, destination_table, database, **kwargs
    ):
        super(WritePandasDfToSQL, self).__init__(**kwargs)
        self.file_path = file_path
        self.sql_conn_id = sql_conn_id
        self.destination_table = destination_table
        self.database = database or "db.chesdata"

    def execute(self, context):
        hook = SqliteHook(sqlite_conn_id=self.sql_conn_id, schema=self.database)
        df = pd.read_csv(self.file_path)
        rows = list(df.itertuples(index=False, name=None))
        print(f"Priniting row[0] => {rows[0]}")
        hook.insert_rows(table=self.destination_table, rows=rows)

