from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import plugins.etl.writetosql as extract
import plugins.etl.download_files as download
import plugins.etl.transform as transform
from plugins.operators.filestosql_operator import WritePandasDfToSQL
import plugins.utils.sqlconn as sqlconn

from plugins.conf import config


default_args = {"start_date": datetime(2022, 4, 27), "owner": "Airflow"}


dag = DAG(
    dag_id="chesdataworkflow",
    schedule_interval="*/55 * * * *",
    default_args=default_args,
    template_searchpath="/opt/airflow/plugins/include/sql",
    catchup=False,
    tags=["chesdata"],
)

# Start process
start_operator = DummyOperator(task_id="start_execution", dag=dag)

# Download the files from chapel hill expert survey links (see the config files) to data folder
task_download_codebook = PythonOperator(
    task_id="download_ches_codebook",
    python_callable=download.download_files,
    op_kwargs={"uri": config.codebook_uri, "file_name": config.codebookfile},
    dag=dag,
)

task_download_chesdata = PythonOperator(
    task_id="download_ches2019data",
    python_callable=download.download_files,
    op_kwargs={"uri": config.chesdata_uri, "file_name": config.chesdatafile},
    dag=dag,
)

task_download_questionnaire = PythonOperator(
    task_id="download_questionnaire",
    python_callable=download.download_files,
    op_kwargs={"uri": config.questionnaire_uri, "file_name": config.questionnairefile},
    dag=dag,
)

# End download operator
end_download_operators = DummyOperator(
    task_id="end_download_task", trigger_rule="none_failed", dag=dag
)

# Create Sqlite connection

task_create_conn = PythonOperator(
    task_id="create_sqlite_conn",
    python_callable=sqlconn.create_airflow_sql_conn,
    op_kwargs={"host": config.docker_db_path, "schema": config.db_schema},
    dag=dag,
)

# Create table by joining Datasets
task_ddls = SqliteOperator(
    task_id="create_tables",
    sqlite_conn_id="sqlite_conn_id",
    sql="ddls.sql",
    dag=dag,
)


# Extract tasks
task_extract_chesdata = PythonOperator(
    task_id="extract_chesdata",
    python_callable=extract.get_and_staged_data,
    op_kwargs={
        "path": config.chesdata_path,
        "table_name": "chesdata",
        "db_uri": config.docker_db_uri,
    },
    dag=dag,
)

# To show the use of custom operators, below snippet is commented. However
# if you want to run below then comment the snippet which use WritePandasDfToSQL.

# task_extract_partydata = PythonOperator(
#     task_id="extract_partydata",
#     python_callable=extract.get_and_staged_data,
#     op_kwargs={"path": config.party_file, "table_name": "party"},
#     dag=dag,
# )

task_extract_partydata = WritePandasDfToSQL(
    task_id="extract_partydata",
    file_path=config.party_file,
    sql_conn_id="sqlite_conn_id",
    destination_table="party",
    database=config.docker_db_path,
    dag=dag,
)

# Transform party data.
task_transform_ffill_party = PythonOperator(
    task_id="transform_ffill_party_data",
    python_callable=transform.apply_transformation,
    op_kwargs={
        "table_name": "party",
        "column_name": "country_abbrev",
        "apply_func": "ffill",
    },
    dag=dag,
)


task_extract_country_abbr = PythonOperator(
    task_id="extract_country_abbr_and_party",
    python_callable=extract._generate_country_abbr_and_party,
    op_kwargs={
        "path": config._chescodebook_path,
        "table_name": "country_abbr",
        "db_uri": config.docker_db_uri,
    },
    dag=dag,
)

end_extration_operators = DummyOperator(
    task_id="end_extration_task", trigger_rule="none_failed", dag=dag
)

# drop merged dataset
task_drop_merged_ds = task_create_dataset_from_joined_data = SqliteOperator(
    task_id="drop_merged_dataset",
    sqlite_conn_id="sqlite_conn_id",
    sql="drop_merge_ds.sql",
    dag=dag,
)

# Create table by joining Datasets
task_create_dataset_from_joined_data = SqliteOperator(
    task_id="create_merged_dataset",
    sqlite_conn_id="sqlite_conn_id",
    sql="create_merge_ds.sql",
    dag=dag,
)

end_execution = DummyOperator(
    task_id="end_execution", trigger_rule="none_failed", dag=dag
)

# Structuring DAG

# Download files execution
(
    start_operator
    >> [task_download_chesdata, task_download_codebook, task_download_questionnaire]
    >> end_download_operators
)

# Create Sql connection and create tables
end_download_operators >> task_create_conn >> task_ddls

# Extracting and staging into SQL
(
    task_ddls
    >> [task_extract_chesdata, task_extract_partydata, task_extract_country_abbr]
    >> end_extration_operators
)

# Transform
end_extration_operators >> task_transform_ffill_party

# Join all three datasets and dump into SQL
(
    task_transform_ffill_party
    >> task_drop_merged_ds
    >> task_create_dataset_from_joined_data
    >> end_execution
)
