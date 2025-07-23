import os
import datetime
import psycopg2
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Переменные окружения Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

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

# Использование Playwright
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/115.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        page.goto("https://kaspi.kz/shop/c/shoes/?page=1", timeout=60000)
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        print("✅ Страница Kaspi получена")
        browser.close()

except Exception as e:
    print("❌ Ошибка при получении страницы через Playwright:", e)
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
