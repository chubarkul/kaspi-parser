import asyncio
import os
import re
import json
import psycopg2
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("❌ DATABASE_URL не задана в переменных окружения")

    # Добавим sslmode=require, если его нет
    if "sslmode" not in db_url:
        if "?" in db_url:
            db_url += "&sslmode=require"
        else:
            db_url += "?sslmode=require"

    try:
        conn = psycopg2.connect(db_url)
        print("✅ Подключение к базе установлено")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к базе: {e}")
        return None


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


async def fetch_html_with_playwright(url: str) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle")
            html = await page.content()
            await browser.close()
            return html
    except Exception as e:
        print(f"❌ Ошибка при получении страницы через Playwright: {e}")
        return ""


def extract_products_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    script_tag = soup.find("script", text=re.compile("productListData"))

    if not script_tag:
        print("⚠️ Не найден скрипт с productListData")
        return []

    try:
        json_text = re.search(r'window\.productListData\s*=\s*(\{.*?\});', script_tag.string, re.DOTALL)
        if not json_text:
            print("⚠️ Не удалось извлечь JSON из скрипта")
            return []

        data = json.loads(json_text.group(1))
        products = data.get("catalogListItems", [])
        result = []
        for item in products:
            title = item.get("title")
            url = f"https://kaspi.kz/shop/p/{item.get('url')}" if item.get("url") else None
            if title and url:
                result.append((title.strip(), url.strip()))
        return result
    except Exception as e:
        print(f"❌ Ошибка при парсинге JSON: {e}")
        return []


def save_to_db(conn, products):
    with conn.cursor() as cur:
        for title, url in products:
            cur.execute(
                "INSERT INTO kaspi_products (title, url) VALUES (%s, %s) ON CONFLICT (url) DO NOTHING",
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
    html = await fetch_html_with_playwright(url)
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
