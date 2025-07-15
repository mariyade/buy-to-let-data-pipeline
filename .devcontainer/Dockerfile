# .devcontainer/Dockerfile

FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y \
    git \
    curl \
    vim \
    gcc \
    libpq-dev \
    && apt-get clean

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
