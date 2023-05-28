
import os
import csv
import re
import requests
from bs4 import BeautifulSoup

# Функция для поиска наборов данных
def find_dataset(text):
    pattern = r'\b\w+\.txt\b'
    matches = re.findall(pattern, text)
    return matches

# Создание папки для данных
def make_folder():
    folder_name = "data"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

# Получение HTML-контента страницы
online = requests.get("https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm")
soup = BeautifulSoup(online.content, "html.parser")
find = soup.findAll("li", {"class": "MsoNormal"})
i = 0

cities = []
regions = []
realRegions = []

# Извлечение информации о городах и регионах
city_list = soup.findAll("li", {"class": "MsoNormal"})
for item in city_list:
    city = item.text.partition('(')[0].strip()
    cities.append(city)

    region = item.find_previous("p").text.strip().replace("\n", " ")
    if region:
        regions.append(region)

    region_span = item.find_previous("span", style="mso-bidi-font-size:10.0pt;font-family:Arial")
    if not region_span:
        region_span = item.find_previous("span", style=re.compile(r".*color:maroon.*"))
    region = region_span.text if region_span else ""
    realRegions.append(region)

data = []

set_south_america_region = False

# Обработка найденных наборов данных
for item in find:
    result = find_dataset(item.text)
    osn = ''.join(result)
    dataset = osn[:len(osn) - 4]

    countries = regions[i]

    region_span = item.find("span", style="mso-bidi-font-size:10.0pt;font-family:Arial")
    if not region_span:
        region_span = item.find("span", style=re.compile(r".*color:maroon.*"))
    region = realRegions[i].replace("\n", " ").replace("\r", "")
    country_spans = item.findAll("span", style=re.compile(r".*color:maroon.*"))
    for span in country_spans:
        country = re.sub(r'[\[\]\r\n]', '', span.text).strip().replace("\n", " ").replace("\r", "")
        if country:
            countries += ", " + country

    if region == "":
        region = realRegions[i - 1].replace("\n", " ").replace("\r", "")
        realRegions[i] = region

    if "Argentina" in countries:
        set_south_america_region = True
    if set_south_america_region:
        region = "South/Central America & Carribean"

    if region or countries or cities[i] or dataset:
        data.append([region.replace("\n", " ").replace("\r", ""),
                     countries.replace("\n", " ").replace("\r", ""),
                     cities[i].replace("\n", " ").replace("\r", ""),
                     dataset])

    i += 1

make_folder()

filename = "data/data.csv"

# Запись данных в CSV-файл
with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["region", "country", "city", "data"])
    writer.writerows(data)

print("<<Работа выполнена>>")
