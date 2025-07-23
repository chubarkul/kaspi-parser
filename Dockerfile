# Используем официальный образ Playwright
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Устанавливаем зависимости
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем твой код
COPY . .

# Устанавливаем браузеры
RUN playwright install chromium

# Запуск скрипта
CMD ["python", "parser.py"]
