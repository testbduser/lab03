import os
import requests
import zipfile
from tqdm import tqdm

# Функция для скачивания береговой линии
def downloadCoastline():
    url = "https://naciscdn.org/naturalearth/10m/physical/ne_10m_coastline.zip"  # URL для скачивания береговой линии

    folder = "coastline"  # Папка для сохранения данных

    if not os.path.exists(folder):  # Проверяем, существует ли папка
        os.makedirs(folder)  # Если не существует, создаем папку
    else:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):  # Если в папке есть файлы, удаляем их
                os.remove(file_path)

    response = requests.get(url, stream=True)  # Отправляем GET-запрос для получения данных
    total_size = int(response.headers.get("content-length", 0))  # Получаем размер данных
    block_size = 1024  # Размер блока для записи
    progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)  # Создаем прогресс-бар

    with open(os.path.join(folder, "ne_10m_coastline.zip"), "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))  # Обновляем прогресс-бар
            file.write(data)  # Записываем данные в файл

    progress_bar.close()
    print("Распаковка архива")  # Выводим сообщение о распаковке архива
    zip_path = os.path.join(folder, "ne_10m_coastline.zip")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(folder)  # Распаковываем архив

    os.remove(zip_path)  # Удаляем скачанный архив


# Функция для скачивания населенных пунктов
def downloadPopulatedPlaces():
    url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip"  # URL для скачивания населенных пунктов

    folder = "populated_places"  # Папка для сохранения данных

    if not os.path.exists(folder):  # Проверяем, существует ли папка
        os.makedirs(folder)  # Если не существует, создаем папку
    else:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):  # Если в папке есть файлы, удаляем их
                os.remove(file_path)

    response = requests.get(url, stream=True)  # Отправляем GET-запрос для получения данных
    total_size = int(response.headers.get("content-length", 0))  # Получаем размер данных
    block_size = 1024  # Размер блока для записи
    progress_bar = tqdm(total=total_size, unit="B", unit_scale=True)  # Создаем прогресс-бар

    with open(os.path.join(folder, "ne_10m_populated_places.zip"), "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))  # Обновляем прогресс-бар
            file.write(data)  # Записываем данные в файл

    progress_bar.close()
    print("Распаковка архива")  # Выводим сообщение о распаковке архива
    zip_path = os.path.join(folder, "ne_10m_populated_places.zip")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(folder)  # Распаковываем архив

    os.remove(zip_path)  # Удаляем скачанный архив


if not os.path.exists("coastline"):  # Если папки с береговой линией не существует, скачиваем
    downloadCoastline()

if not os.path.exists("populated_places"):  # Если папки с населенными пунктами не существует, скачиваем
    downloadPopulatedPlaces()
