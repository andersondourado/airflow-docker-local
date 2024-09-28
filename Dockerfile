FROM apache/airflow:2.9.3-python3.9

COPY requirements.txt .
# install your pip packages
RUN pip install -r requirements.txt