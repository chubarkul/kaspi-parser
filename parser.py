import asyncio
import os
import re
import json
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.async_api import async_playwright


def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("❌ DATABASE_URL не задана в переменных окружения")
    if "sslmode" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    try:
        conn = psycopg2.connect(db_url)
        print("✅ Подключение к базе установлено")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к базе: {e}")
        return None


async def get_page_html(url: str) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            page = await context.new_page()
            await page.goto(url)
            await page.wait_for_timeout(5000)  # ждём 5 сек на всякий случай
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        print(f"❌ Ошибка при получении страницы через Playwright: {e}")
        return ""


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kaspi_products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                url TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    print("✅ Таблица проверена/создана")


def extract_products_from_html(html):
    soup = BeautifulSoup(html, "html.parser")

    # DEBUG: вывод первых 3000 символов
    print("=== Часть HTML страницы (первые 3000 символов) ===")
    print(html[:3000])
    print("=== Конец вывода ===")

    script_tag = soup.find("script", string=re.compile("productListData"))
    if not script_tag:
        print("⚠️ Не найден скрипт с productListData")
        return []

    try:
        json_text_match = re.search(r'"productListData"\s*:\s*(\[\{.*?\}\])', script_tag.string, re.DOTALL)
        if not json_text_match:
            print("⚠️ Не удалось извлечь JSON из скрипта")
            return []

        json_data = json.loads(json_text_match.group(1))
        products = []
        for item in json_data:
            title = item.get("title")
            url = "https://kaspi.kz" + item.get("url", "")
            if title and url:
                products.append((title, url))
        return products

    except Exception as e:
        print(f"⚠️ Ошибка при обработке JSON: {e}")
        return []


def save_to_db(conn, products):
    with conn.cursor() as cur:
        for title, url in products:
            cur.execute(
                "INSERT INTO kaspi_products (title, url) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (title, url)
            )
        conn.commit()
    print(f"✅ Парсер завершён. Добавлено новых товаров: {len(products)}")
    print(f"🕒 Время завершения: {datetime.now()}")


async def main():
    print(f"🔥 Парсер запустился: {datetime.now()}")
    conn = get_db_connection()
    if not conn:
        return

    create_table(conn)

    url = "https://kaspi.kz/shop/c/shoes/?page=1"
    html = await get_page_html(url)
    if not html:
        conn.close()
        return

    products = extract_products_from_html(html)
    if products:
        save_to_db(conn, products)
    else:
        print("⚠️ Завершено без добавления товаров")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
