import os
import datetime
import psycopg2
import requests
import time
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Настройки базы из environment (Render)
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

KASPI_URL = "https://kaspi.kz/shop/c/shoes/?page=1"

print(f"🔥 Парсер запустился: {datetime.datetime.now()}")

# Подключение к базе
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    print("✅ Подключение к базе установлено")
except Exception as e:
    print("❌ Ошибка подключения к базе:", e)
    exit(1)

# Создание таблицы
try:
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shoes (
            id SERIAL PRIMARY KEY,
            name TEXT,
            price TEXT,
            url TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.commit()
    print("✅ Таблица проверена/создана")
except Exception as e:
    print("❌ Ошибка при создании таблицы:", e)
    conn.close()
    exit(1)

# Функция с повторной попыткой при 429
@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=10, max=60),
    stop=stop_after_attempt(5)
)
def get_page():
    ua = UserAgent()
    headers = {"User-Agent": ua.random}
    response = requests.get(KASPI_URL, headers=headers)
    if response.status_code == 429:
        raise Exception("429 Too Many Requests")
    response.raise_for_status()
    return response.text

# Получение страницы
try:
    html = get_page()
    soup = BeautifulSoup(html, "html.parser")
    print("✅ Страница Kaspi получена")
except Exception as e:
    print("❌ Не удалось получить страницу Kaspi:", e)
    conn.close()
    exit(1)

# Извлечение карточек
products = soup.select("div.item-card__info")
print(f"🔍 Найдено товаров: {len(products)}")

inserted = 0
for p in products:
    try:
        name = p.select_one(".item-card__name").get_text(strip=True)
        price = p.select_one(".item-card__prices").get_text(strip=True)
        url = "https://kaspi.kz" + p.select_one("a")["href"]

        cursor.execute("""
            INSERT INTO shoes (name, price, url)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, (name, price, url))
        inserted += 1

        time.sleep(random.uniform(0.2, 0.7))  # немного притормозим на всякий

    except Exception as e:
        print("⚠️ Ошибка при обработке карточки:", e)
        continue

conn.commit()
cursor.close()
conn.close()

print(f"✅ Парсер завершён. Добавлено новых товаров: {inserted}")
print(f"🕒 Время завершения: {datetime.datetime.now()}")
