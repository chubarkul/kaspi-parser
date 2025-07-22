import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime

headers = {
    "User-Agent": "Mozilla/5.0"
}

filename = f"shoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

with open(filename, mode="w", newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Название", "Цена", "Ссылка"])

    for page in range(1, 4):
        url = f"https://kaspi.kz/shop/c/shoes/?page={page}"
        print(f"Парсим: {url}")
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.item-card__info')

        for item in items:
            name_tag = item.select_one('.item-card__name-link')
            price_tag = item.select_one('.item-card__prices')

            name = name_tag.text.strip() if name_tag else "Нет названия"
            link = "https://kaspi.kz" + name_tag['href'] if name_tag else "Нет ссылки"
            price = price_tag.text.strip() if price_tag else "Нет цены"

            writer.writerow([name, price, link])
            print(f"{name} — {price}\n{link}\n")

print(f"\n✅ Парсинг завершён. Данные сохранены в {filename}")
time.sleep(300)  # держим worker живым 5 минут
