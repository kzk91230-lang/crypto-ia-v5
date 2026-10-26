FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p app/static && touch app/__init__.py
COPY *.py app/
COPY index.html app/static/index.html
ENV PYTHONUNBUFFERED=1 DB_PATH=/srv/data/signals.db
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
