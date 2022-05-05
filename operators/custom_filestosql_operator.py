import pandas as pd

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.decorators import apply_defaults


class WritePandasDfToSQL(SqliteOperator):
    """ "
    A custom operator to read csv files and put df into sqlite db.
    :param Path file_path: file or uri
    :param String destination_table: destination_table
    """

    @apply_defaults
    def __init__(self, file_path, sql_conn_id, destination_table, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.sql_conn_id = sql_conn_id
        self.destination_table = destination_table
        self.database="db.chesdata"

    def execute(self, context):
        self.hook = SqliteHook(sqlite_conn_id=self.sql_conn_id, schema=self.database)
        df = pd.read_csv(self.file)
        rows = list(df.itertuples(index=False, name=None))
        self.hook.insert_rows(table=self.destination_table, rows=rows)
