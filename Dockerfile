FROM quay.io/astronomer/astro-runtime:3.0.4
COPY dags/data/ ${AIRFLOW_HOME}/data/
