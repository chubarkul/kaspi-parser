import os
import time
import datetime
import requests
import psycopg2
from bs4 import BeautifulSoup

# Конфигурация из переменных окружения Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Константы
KASPI_URL = "https://kaspi.kz/shop/c/shoes/?page=1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36"
}

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

# Запрос к Kaspi
try:
    response = requests.get(KASPI_URL, headers=HEADERS)
    if response.status_code == 429:
        print("⚠️  Ошибка 429: Слишком много запросов. Пробуем подождать...")
        time.sleep(10)
        response = requests.get(KASPI_URL, headers=HEADERS)

    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    print("✅ Страница Kaspi получена")
except Exception as e:
    print("❌ Ошибка при получении данных с Kaspi:", e)
    conn.close()
    exit(1)

# Поиск карточек товара
products = soup.select("div.item-card__info")
if not products:
    print("⚠️ Не найдено карточек товаров")
else:
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

    except Exception as e:
        print("⚠️ Ошибка при обработке карточки:", e)
        continue

# Завершение
conn.commit()
cursor.close()
conn.close()

print(f"✅ Парсер завершён. Добавлено новых товаров: {inserted}")
print(f"🕒 Время завершения: {datetime.datetime.now()}")
