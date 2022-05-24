import pandas as pd

import utils.sqlconn as sqlconn
from plugins.conf.config import docker_db_uri


def apply_transformation(
    table_name,
    column_name,
    apply_func="ffill",
    db_uri=None,
):
    if db_uri is None:
        db_uri = docker_db_uri
    column_name = f"{column_name}"
    df = pd.read_sql_table(table_name, db_uri)
    if apply_func == "ffill":
        df[column_name] = df[column_name].replace({"nan": None}).ffill()
    else:
        raise ValueError("Only ffill is supported as of now :) ")
    sqlconn.write_to_sql(df, table_name, db_uri)
