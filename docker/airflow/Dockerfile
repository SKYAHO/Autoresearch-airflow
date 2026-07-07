FROM quay.io/astronomer/astro-runtime:13.8.0

# The DAGs are synced by git-sync. This tiny helper package is stable runtime glue
# imported by DAG files to build KubernetesPodOperator arguments.
COPY autoresearch_airflow /usr/local/airflow/autoresearch_airflow

ENV PYTHONPATH=/usr/local/airflow
