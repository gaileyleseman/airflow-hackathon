FROM apache/airflow:3.1.0

USER root
RUN groupadd -g 984 docker 2>/dev/null || true && usermod -aG docker airflow
USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker \
    apache-airflow-providers-fab \
    apache-airflow-providers-amazon
