from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import operators.writetosql as extract
import operators.download_files as download
import operators.transform as transform
from operators.filestosql_operator import WritePandasDfToSQL
import utils.sqlconn as sqlconn

from conf import config


default_args = {"start_date": datetime(2022, 4, 27), "owner": "Airflow"}


dag = DAG(
    dag_id="chesdataworkflow",
    schedule_interval="*/55 * * * *",
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/include/sql",
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
    op_kwargs={"path": config.chesdata_path, "table_name": "chesdata"},
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
    database="db.chesdata",
    dag=dag,
)

# Transform party data.
task_transform_ffill_party = PythonOperator(
    task_id="transform_ffill_party_data",
    python_callable=transform.apply_transformation, 
    op_kwargs={
        "table_name" : "party",
        "column_name":"country_abbrev",
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
    },
    dag=dag,
)

end_extration_operators = DummyOperator(
    task_id="end_extration_task", trigger_rule="none_failed", dag=dag
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
start_operator >> task_download_chesdata >> end_download_operators
start_operator >> task_download_codebook >> end_download_operators
start_operator >> task_download_questionnaire >> end_download_operators

# Create Sql connection and create tables
end_download_operators >> task_create_conn >> task_ddls

# Extracting and staging into SQL
task_ddls >> task_extract_chesdata >> end_extration_operators
task_ddls >> task_extract_partydata >> end_extration_operators
task_ddls >> task_extract_country_abbr >> end_extration_operators

# Transform 
end_extration_operators >> task_transform_ffill_party

# Join all three datasets and dump into SQL
task_transform_ffill_party >> task_create_dataset_from_joined_data >> end_execution
