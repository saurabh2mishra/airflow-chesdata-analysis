import pandas as pd

import utils.sqlconn as sqlconn

def apply_transformation(table_name, column_name, apply_func="ffill", db_uri="sqlite:///db.chesdata"):
    column_name=f"{column_name}"
    df = pd.read_sql_table(table_name, db_uri)
    if apply_func == "ffill":     
        df = df.mask(df[column_name]=='nan', None).ffill()
    else:
        raise ValueError("Only ffill is supported as of now :) ")
    sqlconn.write_to_sql(df, table_name=table_name)