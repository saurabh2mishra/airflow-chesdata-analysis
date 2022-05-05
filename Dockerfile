FROM apache/airflow:2.2.5
USER root

# Install openjdk for tabula to read the pdf tabular data.
# RUN apt-get update -y && \
#     apt-get install -y openjdk-11-jre-headless && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/* ;

USER airflow

# Install python dependecies 
COPY requirements.txt ./requirements.txt
RUN pip3 install --user --upgrade pip
RUN pip3 install --no-cache-dir --user -r requirements.txt
