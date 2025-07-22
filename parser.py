import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Конфиг из .env или окружения Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

KASPI_URL = "https://kaspi.kz/shop/c/shoes/?page=1"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36"
}

def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("✅ Подключение к базе установлено")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к базе: {e}")
        return None

def fetch_kaspi_data():
    try:
        response = requests.get(KASPI_URL, headers=HEADERS, timeout=10)
        if response.status_code == 429:
            print("❌ Ошибка 429: слишком много запросов. Ждём и пробуем снова...")
            time.sleep(10)
            return fetch_kaspi_data()
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"❌ Ошибка при получении данных с Kaspi: {e}")
        return None

def main():
    print(f"🔥 Парсер запустился: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    conn = connect_db()
    if not conn:
        return

    html = fetch_kaspi_data()
    if not html:
        return

    # здесь можно добавить парсинг и сохранение в базу
    print("✅ Данные успешно получены. Можем парсить и сохранять.")

    conn.close()

if __name__ == "__main__":
    main()
