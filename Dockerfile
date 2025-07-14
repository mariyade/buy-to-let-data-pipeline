FROM quay.io/astronomer/astro-runtime:12.1.0

COPY data/ ${AIRFLOW_HOME}/data/
