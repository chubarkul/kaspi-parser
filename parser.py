import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import psycopg2
import datetime

# Загружаем переменные из .env
load_dotenv()

# Подключение к базе
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

    cursor = conn.cursor()
except Exception as e:
    print("❌ Ошибка подключения к базе:", e)
    exit(1)

# Создаём таблицу, если ещё нет
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
except Exception as e:
    print("❌ Ошибка при создании таблицы:", e)
    exit(1)

# Парсим первую страницу каталога
headers = {
    "User-Agent": "Mozilla/5.0"
}
url = "https://kaspi.kz/shop/c/shoes/?page=1"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
except Exception as e:
    print("❌ Ошибка при получении данных с Kaspi:", e)
    exit(1)

# Ищем карточки товаров
products = soup.select("div.item-card__info")

if not products:
    print("⚠️ Не удалось найти карточки товаров.")
else:
    print(f"🔎 Найдено товаров: {len(products)}")

# Извлекаем и вставляем данные
inserted = 0

for p in products:
    try:
        name = p.select_one(".item-card__name").get_text(strip=True)
        price = p.select_one(".item-card__prices").get_text(strip=True)
        link = "https://kaspi.kz" + p.select_one("a")["href"]

        cursor.execute("""
            INSERT INTO shoes (name, price, url)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO NOTHING
        """, (name, price, link))
        inserted += 1

    except Exception as e:
        print("⚠️ Ошибка при обработке карточки:", e)
        continue

# Сохраняем и закрываем
conn.commit()
cursor.close()
conn.close()

print(f"✅ Парсер завершён. Добавлено новых товаров: {inserted}")
print("🕒 Время завершения:", datetime.datetime.now())
