FROM apache/airflow:slim-3.1.6-python3.13

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -r /requirements.txt