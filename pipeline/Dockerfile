FROM python:3.12-slim

WORKDIR /app

COPY pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code du pipeline, puis tests (et compose, lu par le test d'architecture)
COPY pipeline/runner.py .
COPY pipeline/marketplace/ marketplace/
COPY tests/ tests/
COPY docker-compose.yml .

CMD ["python", "runner.py", "serve"]
