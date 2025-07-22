import psycopg2

# Подключение к Supabase PostgreSQL
conn = psycopg2.connect(
    host="aws-0-eu-central-1.pooler.supabase.com",
    port=6543,
    database="postgres",
    user="postgres.iskfuiszpxwrdptbufqc",
    password="Thisissparta302@"  #исправить!
)

cursor = conn.cursor()

# Создаём таблицу, если ещё нет
cursor.execute("""
CREATE TABLE IF NOT EXISTS shoes (
    id SERIAL PRIMARY KEY,
    name TEXT,
    price TEXT,
    url TEXT,
    created_at TIMESTAMP DEFAULT now()
)
""")

# Пример данных — замени на свои из парсера
shoes_data = [
    {"name": "Nike Air Max", "price": "49 990 ₸", "url": "https://kaspi.kz/shop/p/nike-1"},
    {"name": "Adidas Ultraboost", "price": "64 000 ₸", "url": "https://kaspi.kz/shop/p/adidas-2"}
]

# Вставляем каждую пару
for shoe in shoes_data:
    cursor.execute("""
        INSERT INTO shoes (name, price, url)
        VALUES (%s, %s, %s)
    """, (shoe["name"], shoe["price"], shoe["url"]))

# Сохраняем и закрываем
conn.commit()
cursor.close()
conn.close()
