import asyncio
import os
import json
import psycopg2
from datetime import datetime
from playwright.async_api import async_playwright

CATEGORY_URL = "https://kaspi.kz/shop/c/shoes/"
MAX_PAGES = 5


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


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS kaspi_products (
                id SERIAL PRIMARY KEY,
                title TEXT,
                url TEXT UNIQUE,
                price BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    print("✅ Таблица проверена/создана")


async def get_product_list_from_page(page, page_num):
    url = f"{CATEGORY_URL}?page={page_num}"
    print(f"🌐 Открываем: {url}")
    await page.goto(url, timeout=60000)

    # Печать начала HTML-документа
    html = await page.content()
    print(f"\n=== HTML-фрагмент страницы {page_num} ===\n")
    print(html[:1000])
    print(f"\n=== Конец HTML-фрагмента ===\n")

    # Ждём window.__KASPIPAGE__
    for _ in range(20):
        result = await page.evaluate("window.__KASPIPAGE__ || null")
        if result:
            try:
                return result['data']['catalogModel']['productList']
            except Exception as e:
                print(f"⚠️ Ошибка чтения productList: {e}")
                return []
        await page.wait_for_timeout(500)

    print("⚠️ Не дождались __KASPIPAGE__")
    return []


def save_to_db(conn, products):
    with conn.cursor() as cur:
        for product in products:
            title = product.get("name")
            url = "https://kaspi.kz" + product.get("url", "")
            price = product.get("price")
            if title and url:
                cur.execute("""
                    INSERT INTO kaspi_products (title, url, price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                """, (title, url, price))
        conn.commit()
    print(f"💾 Сохранено товаров: {len(products)}")


async def main():
    print(f"🚀 Парсер запущен: {datetime.now()}")
    conn = get_db_connection()
    if not conn:
        return
    create_table(conn)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/115.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            device_scale_factor=1,
            is_mobile=False,
            has_touch=False,
            locale="ru-RU"
        )

        # Максимальная маскировка: navigator.*
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru']});
        """)

        # Блокировка лишних ресурсов
        await context.route("**/*", lambda route, request: (
            route.abort()
            if request.resource_type in ["image", "media", "font"]
            or any(x in request.url for x in ["adfox", "google-analytics", "KaspiApp"])
            else route.continue_()
        ))

        page = await context.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            products = await get_product_list_from_page(page, page_num)
            if not products:
                print(f"🛑 Страница {page_num} пуста, выходим")
                break
            save_to_db(conn, products)

        await browser.close()

    conn.close()
    print(f"🏁 Готово: {datetime.now()}")


if __name__ == "__main__":
    asyncio.run(main())
