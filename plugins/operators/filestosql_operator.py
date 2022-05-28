import pandas as pd

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class WritePandasDfToSQL(BaseOperator):
    """
    A custom operator to read csv files and put df into sqlite db.
    :param Path file_path: file or uri
    :param String destination_table: destination_table
    """

    ui_color = "#48D232"
    ui_fgcolor ="#E52B50"

    @apply_defaults
    def __init__(self, file_path, sql_conn_id, destination_table, database, **kwargs):
        super(WritePandasDfToSQL, self).__init__(**kwargs)
        self.file_path = file_path
        self.sql_conn_id = sql_conn_id
        self.destination_table = destination_table
        self.database = database or "db.chesdata"

    def execute(self, context):
        hook = SqliteHook(sqlite_conn_id=self.sql_conn_id, schema=self.database)
        df = pd.read_csv(self.file_path)
        rows = list(df.itertuples(index=False, name=None))
        # Avoiding duplicate run for the table. Data is static, so for each run no new records will appear.
        # That's why we first truncate and then insert the records. ** There is no truncate command in sqlite.

        hook.run(f"DELETE FROM {self.destination_table};")
        # hook.run(f"UPDATE SQLITE_SEQUENCE SET seq = 0 WHERE name = '{self.destination_table}';")
        hook.insert_rows(table=self.destination_table, rows=rows)
