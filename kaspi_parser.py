import asyncio
import os
import json
from datetime import datetime
from playwright.async_api import async_playwright
import psycopg2

CATEGORY_URL = "https://kaspi.kz/shop/c/shoes/"
MAX_PAGES = 1
COOKIES_JSON = os.getenv("KASPI_COOKIES_JSON")  # Строка, не файл!


def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("❌ DATABASE_URL не задана")
    if "sslmode" not in db_url:
        db_url += ("&sslmode=require" if "?" in db_url else "?sslmode=require")
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
        for p in products:
            cur.execute("""
                INSERT INTO kaspi_products (title, url)
                VALUES (%s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (p["title"], p["url"]))
        conn.commit()
    print(f"✅ Сохранено в БД: {len(products)}")


async def prepare_context(playwright):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        ),
        locale="ru-RU",
        timezone_id="Asia/Almaty",
        viewport={"width": 1280, "height": 800},
    )

    if COOKIES_JSON:
        try:
            cookies = json.loads(COOKIES_JSON)
            await context.add_cookies(cookies)
            print("🍪 Куки загружены в контекст")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки cookies: {e}")

    # Anti-bot JS подмена
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
    """)

    return browser, context


async def get_products_from_page(page, page_num):
    url = f"{CATEGORY_URL}?page={page_num}&c=750000000"
    print(f"🌐 Открываем: {url}")
    await page.goto(url, timeout=60000)

    # Проверим, появился ли попап
    popup = await page.query_selector(".city-selector__popup")
    if popup:
        print("📍 Попап с городом найден — пытаемся нажать")
        try:
            almaty_btn = await page.query_selector("text=Алматы")
            if almaty_btn:
                await almaty_btn.click()
                await page.wait_for_timeout(1000)
                print("✅ Город выбран: Алматы")
        except:
            print("⚠️ Не удалось выбрать город")
    else:
        print("ℹ️ Попап с городом не появился")

    await page.wait_for_timeout(3000)

    items = await page.query_selector_all(".item-card__name")
    print(f"🔍 Найдено карточек: {len(items)}")

    products = []
    for i, item in enumerate(items):
        try:
            link = await item.query_selector("a")
            title = await link.inner_text()
            href = await link.get_attribute("href")
            if href:
                full_url = "https://kaspi.kz" + href
                products.append({"title": title.strip(), "url": full_url})
                print(f"{i+1}. 🔗 {title.strip()} → {full_url}")
        except:
            continue

    return products


async def main():
    print(f"🚀 Старт: {datetime.now()}")
    conn = get_db_connection()
    create_table(conn)

    async with async_playwright() as p:
        browser, context = await prepare_context(p)
        page = await context.new_page()

        for page_num in range(1, MAX_PAGES + 1):
            products = await get_products_from_page(page, page_num)
            if not products:
                print("🛑 Страница 1 пуста")
                break
            save_to_db(conn, products)

        print("⏸ Браузер открыт. Нажми Enter чтобы завершить...")
        input()
        await browser.close()
        conn.close()
        print(f"🏁 Готово: {datetime.now()}")


if __name__ == "__main__":
    asyncio.run(main())
