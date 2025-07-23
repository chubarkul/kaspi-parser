import asyncio
import os
import psycopg2
from datetime import datetime
from playwright.async_api import async_playwright

CATEGORY_URL = "https://kaspi.kz/shop/c/shoes/"
MAX_PAGES = 5

def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("❌ DATABASE_URL не задана")
    if "sslmode" not in db_url:
        db_url += "&sslmode=require" if "?" in db_url else "?sslmode=require"
    return psycopg2.connect(db_url)

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

async def get_product_list_from_page(page, page_num):
    url = f"{CATEGORY_URL}?page={page_num}"
    print(f"🌐 Загружаем: {url}")
    await page.goto(url, timeout=60000)
    html = await page.content()
    print(f"\n🔍 HTML начало:\n{html}\n")

    import re
    matches = re.findall(r'<a[^>]+href="(/shop/p/[^"]+)"[^>]*>([^<]+)</a>', html)
    products = []
    for href, title in matches:
        full_url = f"https://kaspi.kz{href}"
        products.append((title.strip(), full_url))
    return products

def save_to_db(conn, products):
    with conn.cursor() as cur:
        for title, url in products:
            cur.execute("""
                INSERT INTO kaspi_products (title, url)
                VALUES (%s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (title, url))
        conn.commit()
    print(f"💾 Сохранено: {len(products)} товаров")

async def main():
    print(f"🚀 Старт: {datetime.now()}")
    conn = get_db_connection()
    create_table(conn)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (compatible; YaDirectFetcher/1.0; Dyatel; +http://yandex.com/bots)",
            viewport={"width": 1280, "height": 800},
            locale="ru-RU",
            is_mobile=False,
            has_touch=False
        )
        page = await context.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            products = await get_product_list_from_page(page, page_num)
            if not products:
                print(f"🛑 Страница {page_num} пуста — выходим")
                break
            save_to_db(conn, products)

        await browser.close()
    conn.close()
    print(f"🏁 Готово: {datetime.now()}")

if __name__ == "__main__":
    asyncio.run(main())
