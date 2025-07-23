import asyncio
import os
import psycopg2
from datetime import datetime
from playwright.async_api import async_playwright

CATEGORY_URL = "https://kaspi.kz/shop/c/shoes/"
MAX_PAGES = 1


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


def save_to_db(conn, products):
    with conn.cursor() as cur:
        for product in products:
            cur.execute("""
                INSERT INTO kaspi_products (title, url)
                VALUES (%s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (product["title"], product["url"]))
        conn.commit()
    print(f"✅ Сохранено в БД: {len(products)}")


async def get_product_list_from_page(page, page_num):
    url = f"{CATEGORY_URL}?page={page_num}"
    print(f"🌐 Открываем: {url}")
    await page.goto(url, timeout=60000)

    try:
        await page.wait_for_selector('text=Алматы', timeout=5000)
        await page.click('text=Алматы')
        print("📍 Город выбран: Алматы")
        await page.wait_for_timeout(2000)
    except:
        print("ℹ️ Попап с городом не появился")

    items = await page.query_selector_all(".item-card__name")
    print(f"🔍 Найдено карточек: {len(items)}")

    products = []
    for item in items:
        link = await item.query_selector("a")
        if not link:
            continue
        title = await link.inner_text()
        href = await link.get_attribute("href")
        if href:
            products.append({
                "title": title.strip(),
                "url": "https://kaspi.kz" + href
            })
    return products


async def main():
    print(f"🚀 Старт: {datetime.now()}")
    conn = get_db_connection()
    create_table(conn)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            locale="ru-RU",
            extra_http_headers={"accept-language": "ru-RU,ru;q=0.9"}
        )
        page = await context.new_page()

        all_products = []
        for page_num in range(1, MAX_PAGES + 1):
            products = await get_product_list_from_page(page, page_num)
            if not products:
                print(f"🛑 Страница {page_num} пуста")
                break
            all_products.extend(products)

        save_to_db(conn, all_products)
        await browser.close()
    conn.close()
    print(f"🏁 Готово: {datetime.now()}")


if __name__ == "__main__":
    asyncio.run(main())
