FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Airflow DAGs and Scrapy code
COPY airflow /app/airflow
COPY scrapy_project /app/scrapy_project

ENV AIRFLOW_HOME=/app/airflow

# Default command (Airflow scheduler & webserver start)
CMD ["bash", "-c", "airflow db init && airflow users create --username admin --password admin --firstname Amjad --lastname CP --role Admin --email admin@example.com && airflow scheduler & airflow webserver -p 8080"]
