#!/bin/bash
until pg_isready -h postgres -p 5432 > /dev/null 2>&1; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow webserver & airflow scheduler
