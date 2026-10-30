FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY templates templates

CMD gunicorn --bind 0.0.0.0:${DASHBOARD_PORT} --workers 2 --threads 4 app:app
