# check=skip=SecretsUsedInArgOrEnv
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY ./pipeline ./pipeline

ENV DB_HOST=timescaledb
ENV DB_PORT=5432
ENV DB_NAME=agrotrace_db
ENV DB_USER=admin
ENV DB_PASSWORD=password

CMD ["python", "-m", "pipeline.orchestrator"]
