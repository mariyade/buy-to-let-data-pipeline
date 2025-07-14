FROM quay.io/astronomer/astro-runtime:3.0.4
COPY data/ ${AIRFLOW_HOME}/data/
