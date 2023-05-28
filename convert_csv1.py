import os
import csv
import shutil

def convert_txt_to_csv(input_folder, output_folder):
    # Проверяем, существует ли папка назначения
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)  # Если существует, удаляем ее со всем содержимым
    os.makedirs(output_folder)  # Создаем новую папку назначения

    txt_files = []
    # Ищем все .txt файлы в исходной папке и ее подпапках
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.txt'):
                txt_files.append(os.path.join(root, file))  # Добавляем пути к .txt файлам в список

    for txt_file in txt_files:
        # Создаем путь к .csv файлу в папке назначения на основе имени .txt файла
        csv_file = os.path.join(output_folder, os.path.splitext(os.path.basename(txt_file))[0] + '.csv')

        data = []
        # Открываем .txt файл для чтения
        with open(txt_file, 'r') as file:
            for line in file:
                row = line.strip().split()  # Разделяем строку на отдельные значения
                if len(row) >= 4:
                    city_id = int(row[0])  # Преобразуем идентификатор города в целое число
                    mark_timestamp = "-".join([row[2], row[0], row[1]])  # Формируем метку времени в нужном формате

                    if row[1] != '0':
                        temperature = float(row[3])  # Преобразуем температуру в число с плавающей запятой
                        data.append([city_id, mark_timestamp, temperature])  # Добавляем данные в список

        # Открываем .csv файл для записи
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["city", "mark", "temperature"])  # Записываем заголовки столбцов
            writer.writerows(data)  # Записываем данные в .csv файл

    print(f"Данные из файлов .txt успешно сконвертированы в .csv и сохранены в папку '{output_folder}'.")

input_folder = 'dataset'
output_folder = 'dataset/output_csv'
convert_txt_to_csv(input_folder, output_folder)

