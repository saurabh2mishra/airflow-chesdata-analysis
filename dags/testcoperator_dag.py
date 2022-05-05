from datetime import datetime
from airflow import DAG, settings
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import conf.config as config
import utils.sqlconn as sqlconn
from operators.filestosql_operator import WritePandasDfToSQL

default_args = {"start_date": datetime(2022, 4, 27), "owner": "Airflow"}


dag = DAG(
    dag_id="test_connection",
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/include/sql",
    catchup=False,
    tags=["test_custom_operator"],
)

task_start = DummyOperator(task_id="start_connection_test", dag=dag)


task_create_conn = PythonOperator(
    task_id="create_sqlite_conn",
    python_callable=sqlconn.create_airflow_sql_conn,
    dag=dag,
)

# Create table by joining Datasets
task_ddls = SqliteOperator(
    task_id="create_tables",
    sqlite_conn_id="sqlite_conn_id",
    sql="ddls.sql",
    dag=dag,
)

task_download_codebook = WritePandasDfToSQL(
    task_id="custom_operator_df_to_sql",
    file_path=config.party_file,
    sql_conn_id="sqlite_conn_id",
    destination_table="party",
    database=None,
    dag=dag,
)

task_start >> task_create_conn >> task_ddls >> task_download_codebook
