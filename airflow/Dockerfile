
FROM apache/airflow:2.10.5

COPY pipeline/requirements.txt /tmp/marketplace-requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/marketplace-requirements.txt
