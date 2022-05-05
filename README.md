# Airflow Starter 🚀
We will use docker to setup airflow, and to experiment ETL and visualization. I have chosen Docker for it's simplicity. Setting up Airflow in normal way could be tricky. If you are willing to experiment with normal setup then feel free to do so. 

Docker set up link can be found here [Repalce the link with righ URL](link)

## Starting dokcer airflow project

`docker-compose up`

`./airflow.sh info`

You can also use bash as parameter to enter interactive bash shell in the container or python to enter python container.

`./airflow.sh bash`

`./airflow.sh python`

The webserver is available at: http://localhost:8080. The default account has the login airflow and the password airflow.

## Cleaning up

To stop and delete containers, delete volumes with database data and download images, run:

`docker-compose down --volumes --rmi all`

# Fundamentals of Airflow 

Satisfied principles (not listed are not applicable):

Load data incrementally : extracts only the newly created orders of the day before, not the whole table.

Process historic data : it’s possible to rerun the extract processes, but downstream DAGs have to be started manually.
Enforce the idempotency constraint : every DAG cleans out data if required and possible. Rerunning the same DAG multiple times has no undesirable side effects like duplication of the data.

Rest data between tasks : The data is in persistent storage before and after the operator.

Pool your resources : All task instances in the DAG use a pooled connection to the DWH by specifying the pool parameter.

Manage login details in one place : Connection settings are maintained in the Admin menu.

Develop your own workflow framework : A subdirectory in the DAG code repository contains a framework of operators that are reused between DAGs.

Sense when to start a task : The processing of dimensions and facts have external task sensors which wait until all processing of external DAGs have finished up to the required day.

Specify configuration details once : The place where SQL templates are is configured as an Airflow Variable and looked up as a global parameter when the DAG is instantiated.

## Cron based schedule

```

┌─────── minute (0 - 59)
│ ┌────── hour (0 - 23)
│ │ ┌───── day of the month (1 - 31)
│ │ │ ┌───── month (1 - 12)
│ │ │ │ ┌──── day of the week (0 - 6) (Sunday to Saturday;
│ │ │ │ │      7 is also Sunday on some systems)
* * * * *

```

check this website to generate cron expression - https://www.freeformatter.com/cron-expression-generator-quartz.html

# Best Practices for the task design

### 1- Atomicity 

`either all occur or nothing occurs.` So each task should do only one activity and if not the case then split the functionality into individual task.

### 2- Idempotency

`
Another important property to consider when writing Airflow tasks is idempotency. Tasks are said to be idempotent if calling the same task multiple times with the same inputs has no additional effect. This means that rerunning a task without changing the inputs should not change the overall output.
`

**for data load** : It can be make  idempotent by checking for existing results or making sure that previous results are overwritten by the task.

**for database load** : `upsert` can be used to overwrite  or update previous work done on the tables.


# Back Filling the previous task

The DAG class can be initiated with property `catchup`

if `catchup=False` ->  Airflow starts processing from the `current` interval.

if `catchup=True` -> This is default property and Airflow starts processing from the `past` interval.

# Templating tasks using the Airflow context

All operators load `context` a pre-loaded variable to supply most used variables during DAG run. A python examples can be shown here 

```python
from urllib import request
 
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
 
dag = DAG(
    dag_id="stocksense",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
)
 
def _print_context(**context):
    """
    context here contains these preloaded items 
    to pass in dag during runtime.

    Airflow’s context dictionary can be found in the
    get_template_context method, in Airflow’s models.py.
    
    {
    'dag': task.dag,
    'ds': ds,
    'ds_nodash': ds_nodash,
    'ts': ts,
    'ts_nodash': ts_nodash,
    'yesterday_ds': yesterday_ds,
    'yesterday_ds_nodash': yesterday_ds_nodash,
    'tomorrow_ds': tomorrow_ds,
    'tomorrow_ds_nodash': tomorrow_ds_nodash,
    'END_DATE': ds,
    'end_date': ds,
    'dag_run': dag_run,
    'run_id': run_id,
    'execution_date': self.execution_date,
    'prev_execution_date': prev_execution_date,
    'next_execution_date': next_execution_date,
    'latest_date': ds,
    'macros': macros,
    'params': params,
    'tables': tables,
    'task': task,
    'task_instance': self,
    'ti': self,
    'task_instance_key_str': ti_key_str,
    'conf': configuration,
    'test_mode': self.test_mode,
    }
    """
   start = context["execution_date"]        
   end = context["next_execution_date"]
   print(f"Start: {start}, end: {end}")
 
 
print_context = PythonOperator(
   task_id="print_context", 
   python_callable=_print_context, 
   dag=dag
)
```

## Running docker images as a root

`docker run --rm -it -u root --entrypoint bash apache/airflow:2.2.4`


## Running docker images as a normal user

`docker exec -it <image_id> bash`

*sudo apt-get update && sudo apt-get install tk*

# All about Airflow !!


This project is to create an ETl project to demonstarate capability of airflow and great expectation (as data validation tool) to show what can we acheive as a Data Engineer and Data Scinetst to perform a batch oriented job. Before we switch our attenntion to develop mode to build this, few outstanding points I'd like highlights about **airflow**

- **First** - Airflow is an orchestration tool; don't perform or overuse PythonOperator or BashOperator for everything. Mind the computations stuff :)
- **Second** - There are almost all operator presents for your work; Before you start building new operator check one more extra time and GOOGLE your operators.
- **Third**- *Never forget points 1 & 2* 😃

# Chapel Hill expert surveys 2019 Data Analysis

## **Project Description**

The Chapel Hill expert surveys estimate party positioning on European integration, ideology and policy issues for national parties in a variety of European countries. 

All relevant data is collected under these files which represents below 

- 
- 
-  


Projet description and idea to fill the details to solve the problem with Airflow and great expectations.

As part of this project, we will create an ETL dag with data validation and once all related and required datasets are loaded then we will build a report to answer some outstanding questions for the datasets.


## Data Sets



## how to run same dag for multiple parameters

# Download the files from chapel hill expert survey to data folder

Code snippets are below

```python
files_uris = {"2019_CHES_codebook.pdf" : config.codebook_uri,
        "CHES2019V3.csv" : config.chesdata_uri, 
        "questionnaire.pdf" :config.questionnaire_uri
        }

for file, uri in enumerate(files_uris):
    file_name = file.split(":")[0]
    task_download_data = PythonOperator(
        task_id=f"download_{file_name}",
        python_callable=download.download_files,
        op_kwargs={"uri": uri, "file_name": file_name},
        dag = dag
    )

    # parallelization of all download tasks.
    # This also can be written as start_operator >> task_download_data >> end_download_operators
    
    chain(start_operator, task_download_data, end_download_operators)
```