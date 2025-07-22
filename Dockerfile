FROM apache/airflow:2.10.0-python3.11

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install pyspark
