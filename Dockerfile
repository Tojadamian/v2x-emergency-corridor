FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=us-east-1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY v2x_service/ ./v2x_service/
COPY rsu_service/ ./rsu_service/
COPY simulator/ ./simulator/
COPY static/ ./static/

EXPOSE 8000

CMD ["uvicorn", "v2x_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
