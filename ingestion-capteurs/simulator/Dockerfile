FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests

COPY http_simulator.py .

CMD ["python", "http_simulator.py"]
