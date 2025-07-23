FROM python:3.11-slim

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y wget gnupg curl ca-certificates \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && playwright install --with-deps chromium

CMD ["python", "kaspi_parser.py"]
