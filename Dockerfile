FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates ./templates

ENV GEOCACHE_DB=/app/geocache.sqlite
ENV NOMINATIM_THROTTLE_SEC=1.1
ENV NOMINATIM_USER_AGENT="kmz2csv/1.0 (contact: your-email@example.com)"

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host=0.0.0.0", "--port=8000"]
