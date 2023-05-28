import os
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

# Функция для создания директории для набора данных
def create_dataset_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Функция для очистки директории с набором данных
def clear_dataset_dir(directory):
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

# Функция для получения содержимого веб-страницы
def get_content(url):
    response = requests.get(url)
    content = response.text
    return content

# Функция для парсинга ссылок с определенным расширением
def parse_links(content, extension):
    soup = BeautifulSoup(content, 'html.parser')

    txt_links = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith(extension)]
    return txt_links

# Функция для загрузки файла
def download_file(url, session, file_name):
    response = session.get(url, stream=True)
    response.raise_for_status()

    with open(file_name, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

# Функция для загрузки файлов с определенным расширением
def download_files(url, extension):
    dataset_dir = 'dataset'

    create_dataset_dir(dataset_dir)  # Создание директории для набора данных
    clear_dataset_dir(dataset_dir)  # Очистка директории набора данных

    content = get_content(url)  # Получение содержимого веб-страницы
    txt_links = parse_links(content, extension)  # Парсинг ссылок

    session = requests.Session()

    for link in tqdm(txt_links, desc='Загрузка файлов', unit='файл'):
        file_url = requests.compat.urljoin(url, link)
        file_name = os.path.join(dataset_dir, os.path.basename(link))

        download_file(file_url, session, file_name)  # Загрузка файла

url = 'https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm'
extension = '.txt'

download_files(url, extension)  # Загрузка файлов
