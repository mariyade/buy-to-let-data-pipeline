FROM quay.io/astronomer/astro-runtime:12.1.0

COPY modules/ ${AIRFLOW_HOME}/modules/
COPY data/ ${AIRFLOW_HOME}/data/
