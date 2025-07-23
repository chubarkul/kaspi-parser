import asyncio
import os
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
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await page.goto(url, timeout=30000)

            # Сохраним страницу и скриншот на случай отладки
            await page.screenshot(path="/tmp/kaspi_debug.png", full_page=True)
            html = await page.content()
            with open("/tmp/kaspi_debug.html", "w", encoding="utf-8") as f:
                f.write(html)

            await page.wait_for_selector("div.item-card__info", timeout=30000)
            await browser.close()
            return html
    except Exception as e:
        print(f"❌ Ошибка при получении страницы через Playwright: {e}")
        return ""

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kaspi_products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    print("✅ Таблица проверена/создана")

def parse_products(html):
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.item-card__info")

    if not cards:
        print("⚠️ Не найдено карточек товаров")
        print("--- HTML начало ---")
        print(soup.prettify()[:3000])  # покажем часть HTML
        print("--- HTML конец ---")
        return []

    products = []
    for card in cards:
        name_tag = card.select_one(".item-card__name a")
        if name_tag:
            title = name_tag.get_text(strip=True)
            url = f"https://kaspi.kz{name_tag['href']}"
            products.append((title, url))
    return products

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

    products = parse_products(html)
    if products:
        save_to_db(conn, products)
    else:
        print("⚠️ Завершено без добавления товаров")

    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
