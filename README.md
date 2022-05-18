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

Why these three tasks, you might ask? Why not download the launches and corresponding pictures in one single task? Or why not split them into five tasks? After all, we have five arrows in John’s plan. These are all valid questions to ask while developing a workflow, but the truth is, there’s no right or wrong answer. There are several points to take into consideration, though, and throughout this book we work out many of these use cases to get a feeling for what is right and wrong. The code for this workflow is as follows.


