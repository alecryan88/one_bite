FROM apache/airflow:2.3.2

COPY requirements.txt .

RUN pip install --no-cache-dir  --upgrade pip
RUN pip install --no-cache-dir  -r requirements.txt