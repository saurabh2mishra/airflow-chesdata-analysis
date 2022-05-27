
**Note** 

❗ <sup> **If you are new to Airflow, I ask you to first read this blog
[fundamental of Airflow](https://github.com/saurabh2mishra/airflow-notes). Once you are done with that, then feel free to proceed further.**
</sup>

---

# Chapel Hill expert surveys 2019 Data Analysis

## **Project Description**

The Chapel Hill expert surveys estimates party positioning on European integration, ideology and policy issues for national parties in various European countries. 

32 EU countries and 277 political parties' data were collected to show a trend from 1999 to 2019. Chapel hill expert survey(CHES) is the longest-running and most extensive expert survey on political parties in Europe.

Below are the data sets collected and generated by the CHES team.

- [Chapel Hill Expert Survey dataset](https://www.chesdata.eu/s/CHES2019V3.csv) - This is survey datasets which countries, parties and thier orientation on different topics.

- [2019_CHES_codebook](https://static1.squarespace.com/static/5975c9bfdb29d6a05c65209b/t/5fa04ec05d3c8218b7c91450/1604341440585/2019_CHES_codebook.pdf) - This dataset provides the data for the 2019 Chapel Hill Expert Survey on the positioning of 277
political parties on political ideology, European integration, and policy positions in 32 countries,
including all EU member states.
    
    -  This PDF data contains country, country abbreviation, party and party's details such as name, abbreviation, id etc.
     
     
        `party.csv` and `country_abbr.csv` datasets are parsed from the pdf and kept in data folder. Ideal way would be if it is parsed from pdf itself but that is somthing #todo work at later stage.
    -   The pdf also contains orientation details on different features. 
    
        For instance, the column `EU_POSITION` represents the overall orientation of the party leadership towards European integration in 2019.
    
                1 = Strongly opposed
                2 = Opposed
                3 = Somewhat opposed
                4 = Neutral
                5 = Somewhat in favor
                6 = In favor
                7 = Strongly in favor


- [Survey Questionnaire](https://static1.squarespace.com/static/5975c9bfdb29d6a05c65209b/t/5ed3029fe080e33f639e6e9a/1590887075513/CHES_UK_Qualtrics.pdf) - This dataset has general questions on various identified features, and this is the end goal for the given dataset. For example if we see one of the questions which asks- 

    **`How would you describe the general position on European integration that the party leadeship took during 2019?`**

    And then, for each country and their respective political parties, we need to find out the orientation on the scale of 1 to 7 to show how they think on this matter?

    42 such questions exist in this questionnaire which we need to answer.

## **Prerequisites**
```diff
- Airflow must be installed and working in your machine.
```
Check it out here for [installation guide](https://github.com/saurabh2mishra/airflow-notes#installing-airflow)

## **Airflow project strcuture.**

Before we start writing our DAG and scripts we must follow the standard folder strcuture to organize
our code base. This is the stanard way to organize our code base.

```tree
.
├── dags
│   └── chesworkflow_dag.py
├── data
│   ├── CHES2019V3.csv
│   ├── country_abbr.csv
│   └── party.csv
├── plugins
│   ├── __init__.py
│   ├── conf
│   │   ├── __init__.py
│   │   └── config.py
│   ├── etl
│   │   ├── __init__.py
│   │   ├── download_files.py
│   │   ├── transform.py
│   │   └── writetosql.py
│   ├── include
│   │   ├── __init__.py
│   │   └── sql
│   │       ├── chesdata_ddl.sql
│   │       ├── create_merge_ds.sql
│   │       ├── drop_merge_ds.sql
│   │       └── party_ddl.sql
│   ├── operators
│   │   ├── __init__.py
│   │   └── filestosql_operator.py
│   └── utils
│       ├── __init__.py
│       ├── sqlconn.py
│       └── vizquery.py
|
├── scripts
│   ├── airflow.sh
│   ├── plot.sh
│   └── run_airflow.sh
|
└── visualization
|    ├── __init__.py
|    └── orientation_plot.py
|
├── Dockerfile
├── README.md
├── docker-compose.yaml
├── requirements.txt
```
This is what suggested and followed by developers, but it's not mandatory.
Check it out [here](https://stackoverflow.com/questions/44424473/airflow-structure-organization-of-dags-and-tasks).
The basic idea is to keep the dag in organised way so that it is understandable by others.

## **Solution Description**

We now understand the project at a high level, that we need to build a report for the questionnaires, and we have few datasets.
There are many ways to solve this problem, but we are more interested in solving it in an Airflow way.

So, to start with the solution, we have to follow a typical ETL workflow (extract-transform-load).
Once all required datasets are loaded and merged correctly, we can build our report on top of that.
Solving it in airflow helps us leverage several inbuilt features such as logging, tracking,
workflow management, etc.


## Let's outline the basic steps to create dataset for visualization

1- Get the all 3 datasets. In config file you can see the download link. Important design aspect here is we
can download all these files in parallel. We will keep it in data folder.

2- We need to persist the dataset in SQL DB. Here I have used sqlite database but feel free to choose any
other db as per your choice. So, at this stage first we need to create connection to create tables so that 
extracted data from files can be loaded.

3- Time to extract the data and write them in sqlite tables. Once again all write action can be make parallel.

4- Data is extracted and now it's time for transformation. We will apply transformation to forward fill country information 
for one of the dataset.

5- At this stage, all datasets are clean, and transformed. We will merged them to create a flat file for our
visualization query.

And here ends the Airflow activities. We have created an ETL flow from end to end.

DAG Execution 
![dag-run](/imgs/dag_run.png)

The created dataflow will look like this 

![chapel-hill-survey-dag-graph](/imgs/project_dag_graph.png)

In the second problem, we will use streamlit to make our interactive visualization.

and the visualization.

![visualization](/imgs/viz.gif)


## How to replicate it in your machine. 
 - Clone the repo
 - Read and set up required software as per prerequiste section.
 - if everything is set then
    - Run `docker-compose up`
    - If docker is up and running and you see airflow uri is coming on log then
    - type `localhost:8080` user and password is `airflow`
    - run chesworkflow_dag or tweak the cron timing ( currently it is 55 mins)
    - Once the dag is successfull then run on your local machine `source scripts/plot.sh`
    - go to `http://localhost:8501` and see your plot for this exercise. 
- To stop docker and remove all mount point, run `docker-compose down --volumes --rmi a`


## How can we improve it further?
- Add some data quality checks
- Add unit test 
- Automation of pdf parsing (pdf parsing is very limited in this code)
- Do comment for any other addition/suggestions.


**Remarks** - Intentionally I have kept many variations within the code to show how we can pretty much use plain python functions or any custom
Operator or inbuilt operator to get the job done. However, if you disagree with some of the implementations which might seem redundant, 
feel free to make it work in your own way. To me, it was just experimentation to check what and how much I can leverage this workflow platform.


**Note** - If you want to extend it further please feel free to do so.


---

<sup> **Thank You for following this project** and being with me till end ❤️. If you like the project and work, please share it so that more people can get advantange of it. 🙏 </sup>