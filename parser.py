import os
import datetime
import requests
import psycopg2
from bs4 import BeautifulSoup

# Переменные окружения Render
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

KASPI_URL = "https://kaspi.kz/shop/c/shoes/?page=1"

cookies = {
    'layout': 'd',
    'dt-i': 'env=production|ssrVersion=v1.19.12|pageMode=catalog',
    'ks.cart': '485316a2-8991-410e-a961-a6de794d82e9',
    'ks.tg': '31',
    'kaspi.storefront.cookie.city': '750000000',
    '_hjSessionUser_283363': 'eyJpZCI6Ijk5Yzk3YjdlLWZjYzItNTAyMS1iYjYzLTNjMDA3Njk0OWYzZiIsImNyZWF0ZWQiOjE3MjkyMzE5NDQ1OTQsImV4aXN0aW5nIjp0cnVlfQ==',
    '_ga': 'GA1.1.1021615651.1736933372',
    'ssaid': '7225c8b0-edbf-11ef-9fd9-43b8d005c46d',
    'test.user.group': '90',
    'test.user.group_exp': '4',
    'test.user.group_exp2': '13',
    '_ga_6273EB2NKQ': 'GS2.1.s1748334151$o3$g1$t1748334653$j0$l0$h0',
    '_ga_0R30CM934D': 'GS2.1.s1752581519$o4$g1$t1752581786$j60$l0$h0',
    'current-action-name': 'Index',
    'locale': 'ru-RU',
    'user-device-type': 'mobile',
    'kaspi-payment-region': '18',
    'popups_new': '%7B%22random%22%3A%221%22%7D',
    '__tld__': 'null',
    'k_stat': '9986dd06-33b5-444c-aedf-70dfd90a5066',
    'kkz-main': '1e7ca3cc68b8addb5238c506d0868434be49defbd446d23f9520f71b91b452f96e8a8d14',
}

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en,ru;q=0.9,tr;q=0.8,ky;q=0.7',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Referer': 'https://kaspi.kz/shop/p/krossovki-998771495-belyi-38-117338029/?c=750000000',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36',
    'sec-ch-ua': '"Chromium";v="136", "YaBrowser";v="25.6", "Not.A/Brand";v="99", "Yowser";v="2.5"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
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
    response = requests.get(KASPI_URL, headers=headers, cookies=cookies)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    print("✅ Страница Kaspi получена")
except Exception as e:
    print("❌ Ошибка при получении данных с Kaspi:", e)
    conn.close()
    exit(1)

# Парсинг карточек
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
