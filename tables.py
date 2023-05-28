import csv
import os
import pandas as pd

data_folder = "data"  # Папка для данных
data_filename = os.path.join(data_folder, "data.csv")  # Путь к файлу данных
region_filename = os.path.join(data_folder, "region.csv")  # Путь к файлу регионов
country_filename = os.path.join(data_folder, "country.csv")  # Путь к файлу стран
city_filename = os.path.join(data_folder, "city.csv")  # Путь к файлу городов
measurement_filename = os.path.join(data_folder, "measurement.csv")  # Путь к файлу измерений
populated_places_filename = "/home/vm/lab03/lab03.1/lab3/testfolder/populated_places/CSV/ne_10m_populated_places.csv"  # Путь к файлу населенных пунктов
output_csv_folder = "/home/vm/lab03/lab03.1/lab3/testfolder/dataset/output_csv/"  # Папка для выходных CSV-файлов

regions = {}  # Словарь регионов
countries = {}  # Словарь стран
cities = {}  # Словарь городов

# Чтение данных из файла data.csv
with open(data_filename, mode='r', encoding='utf-8') as data_file:
    csv_reader = csv.reader(data_file)
    next(csv_reader)  # Пропуск заголовка
    for row in csv_reader:
        region = row[0].strip()  # Название региона
        country = row[1].strip()  # Название страны
        city = row[2].strip()  # Название города
        data = row[3].strip()  # Данные
        if region and country and city and data:
            if region not in regions:
                region_id = len(regions) + 1
                regions[region] = region_id
            if country not in countries:
                country_id = len(countries) + 1
                countries[country] = (country_id, regions[region])
            if city not in cities:
                cities[city] = (countries[country], data)

sorted_regions = sorted(regions.keys())  # Сортированный список регионов
sorted_countries = sorted(countries.keys())  # Сортированный список стран
sorted_cities = sorted(cities.keys())  # Сортированный список городов

# Запись регионов в файл region.csv
with open(region_filename, mode='w', newline='', encoding='utf-8') as region_file:
    writer = csv.writer(region_file)
    writer.writerow(["description", "identidier"])  # Запись заголовка
    for idx, region in enumerate(sorted_regions, start=1):
        writer.writerow([region, idx])

print("Файл region.csv успешно создан в папке 'data'.")

# Запись стран в файл country.csv
with open(country_filename, mode='w', newline='', encoding='utf-8') as country_file:
    writer = csv.writer(country_file)
    writer.writerow(["region", "description", "identidier"])  # Запись заголовка
    for idx, country in enumerate(sorted_countries, start=1):
        country_id, region_id = countries[country]
        writer.writerow([region_id, country, idx])

print("Файл country.csv успешно создан в папке 'data'.")

# Запись городов в файл city.csv

with open(populated_places_filename, mode='r', encoding='utf-8') as places_file, \
        open(city_filename, mode='w', newline='', encoding='utf-8') as city_file:
    places_reader = csv.reader(places_file)
    next(places_reader)  # Пропуск заголовка
    writer = csv.writer(city_file)
    writer.writerow(["identidier", "country", "description", "latitude", "longitude", "dataset"])  # Запись заголовка
    city_id = 1
    unique_cities = set()
    for row in places_reader:
        name = row[0].strip()  # Название населенного пункта
        latitude = row[1].strip()  # Широта
        longitude = row[2].strip()  # Долгота
        if name not in unique_cities:
            unique_cities.add(name)
            if name in cities:
                (country_id, _), data = cities[name]
                writer.writerow([city_id, country_id, name, latitude, longitude, data])
                city_id += 1
            city_id += 1

print("Файл city.csv успешно создан в папке 'data'.")

# Создание отображения идентификаторов городов
city_id_map = {}
city_data = pd.read_csv(city_filename)
for _, row in city_data.iterrows():
    city_id_map[row['description']] = row['identidier']

# Запись измерений в файл measurement.csv
with open(measurement_filename, mode='w', newline='', encoding='utf-8') as measurement_file:
    writer = csv.writer(measurement_file)
    writer.writerow(["city", "mark", "temperature"])  # Запись заголовка
    for city in sorted_cities:
        try:
            city_id = city_id_map[city]
        except KeyError:
            continue
        _, data = cities[city]
        csv_file_path = os.path.join(output_csv_folder, data + ".csv")
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Чтение заголовка
            timestamp_idx = header.index("mark")
            temperature_idx = header.index("temperature")
            mark = os.path.splitext(os.path.basename(csv_file_path))[0]

            for row in csv_reader:
                timestamp = row[timestamp_idx].strip()
                temperature = row[temperature_idx].strip()
                writer.writerow([city_id, mark, temperature])

print("Файл measurement.csv успешно создан в папке 'data'.")

