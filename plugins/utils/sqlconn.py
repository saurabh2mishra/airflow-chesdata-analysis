from airflow import settings
from airflow.models import Connection

from sqlalchemy import create_engine
import pandas as pd


def get_sql_conn(db_uri):
    """
    Accept db uri and returns connection for sql
    :param db_uri: databse uri
    :return: SQL engine
    """
    if db_uri is None:
        raise ValueError("No database uri or path given.")
    engine = create_engine(db_uri, echo=False)
    return engine


def read_from_sql(query, db_uri):
    """
    Read data from SQLite
    :param String query: SQL query
    :return: data from SQLite
    """
    engine = get_sql_conn(db_uri)
    return engine.execute(query).fetchall()


def write_to_sql(df, table_name, db_uri):
    """
    Write pandas df to SQLite
    :param pd.DataFrame df: Pandas dataframe
    :param String table_name: table name
    :return: None
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Require Pandas Dataframe.")

    engine = get_sql_conn(db_uri)
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, index=False, if_exists="replace")


# This function create any db sql connection. But default one is sqlite for this usecsase.
# User and password managment must not be done in this way but just for the sake of simplicity
# and demo, I have left it in params


def create_airflow_sql_conn(
    host,
    schema,
    conn_id="sqlite_conn_id",
    conn_type="sqlite",
    login="airflow",
    pwd="airflow",
    desc="Connection to connect local db",
):
    """
    Write pandas df to SQLite
    :param String conn_id: Pandas dataframe
    :param String conn_type: table name
    :param String host: host name
    :param String login: login
    :param String pwd: password
    :param String desc: connection description.
    :return: None
    """
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=pwd,
        schema=schema,
        description=desc,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )

    if str(conn_name) == str(conn.conn_id):
        print(f"Connection {conn.conn_id} already exists")
        return None

    session.add(conn)
    session.commit()
    print(f"Connection {conn_id} is created")
