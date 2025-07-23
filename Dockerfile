FROM python:3.10-slim

# Системные зависимости для Playwright
RUN apt-get update && apt-get install -y     wget curl unzip fonts-liberation libnss3 libxss1 libasound2 libxshmfence1 libxrandr2     libatk1.0-0 libatk-bridge2.0-0 libcups2 libdbus-1-3 libdrm2 libxcomposite1 libxdamage1     libxfixes3 libxinerama1 libpango-1.0-0 libgtk-3-0     && rm -rf /var/lib/apt/lists/*

# Установка зависимостей Python и Playwright
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN playwright install --with-deps

WORKDIR /app
COPY . /app

CMD ["python", "kaspi_bot_scraper.py"]
