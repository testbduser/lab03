import paramiko
import psycopg2
import csv
import os
import time

hostname = '192.168.122.33'
username = 'postgres'
password = 'password'
database = 'lab03'

conn = None

# Проверка информации о базе данных
def verify_database_info():
    global hostname, username, password, database
    
    print("Информация о бд Postgresql:")
    print(f"host -> {hostname}")
    print(f"user -> {username}")
    print(f"password -> {password}")
    print(f"database -> {database}")
    
    choice = input(" 1 - верная информация || 2 - неверная информация -> ")
    
    if choice == "2":
        hostname = input("host -> ")
        username = input("user -> ")
        password = input("password -> ")
        database = input("database -> ")

# Создание схемы
def create_scheme(scheme_name):
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {scheme_name};"
    cursor = conn.cursor()
    cursor.execute(create_schema_query)
    conn.commit()
    cursor.close()

# Установка file_fdw
def install_file_fdw(schema):
    cursor = conn.cursor()
    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA {schema};")
    conn.commit()
    cursor.close()

# Создание файлового сервера
def create_file_server(schema):
    cursor = conn.cursor()

    drop_server_query = "DROP SERVER IF EXISTS file_server CASCADE"
    cursor.execute(drop_server_query)
    conn.commit()

    create_server_query = f"""
        CREATE SERVER file_server
        FOREIGN DATA WRAPPER file_fdw;
    """
    cursor.execute(create_server_query)
    conn.commit()
    cursor.close()

# Создание таблицы
def create_table(scheme ,table_name, columns):
    cursor = conn.cursor()
    drop_query = f"DROP TABLE IF EXISTS {scheme}.{table_name.format()} CASCADE;"
    cursor.execute(drop_query)
    conn.commit()
    create_query = f"CREATE TABLE {scheme}.{table_name} ({', '.join(columns)})"
    
    cursor.execute(create_query)
    
    conn.commit()
    cursor.close()

# Импорт данных из CSV в таблицу
def import_csv_to_table(schema, csv_file, table_name):
    cursor = conn.cursor()
    
    with open(csv_file, 'r') as file:
        csv_data = csv.reader(file)
        columns = next(csv_data)
        
        insert_query = f"INSERT INTO {schema}.{table_name} VALUES ({', '.join(['%s'] * len(columns))})"
        
        cursor.execute(f"SET search_path TO {schema}")
        
        file.seek(0)
        next(csv_data)
        cursor.copy_from(file, table_name, sep=',', null='', columns=columns)
        
    conn.commit()
    cursor.close()

# Получение списка CSV-файлов
def get_csv_files():
    csv_files = []
    table_dir = '/home/vm/lab03/lab03.1/lab3/testfolder/data/'
    for file_name in os.listdir(table_dir):
        if file_name.endswith('.csv'):
            csv_files.append(os.path.join(table_dir, file_name))
    return csv_files

# Подключение к базе данных
def get_connection():
    print("<<Идет подключение к базе данных Postgresql>>")
    global conn
    attempts = 0
    max_attempts = 5
    delay = 2  
    
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host=hostname,
                database=database,
                user=username,
                password=password
            )
            print("<<Connected>>")
            break
        except psycopg2.Error as e:
            print("ERROR -> ", e)
            attempts += 1
            print(f"Restart after {delay} sec - ")
            time.sleep(delay)

# Проверка информации о базе данных
verify_database_info()

# Подключение к базе данных
get_connection()

if conn is not None:
    print("Создание схемы -> data")
    create_scheme('data')
    install_file_fdw('data')
    create_file_server('data')
    print("Создание таблиц:")
    print("region")
    create_table('data', 'region', ['identidier SERIAL PRIMARY KEY', 'description VARCHAR(50)'])
    print("country")
    create_table('data', 'country', ['identidier SERIAL PRIMARY KEY', 'region INTEGER REFERENCES data.region(identidier)', 'description VARCHAR(50)'])
    print("city")
    create_table('data', 'city', ['identidier SERIAL PRIMARY KEY', 'country INTEGER REFERENCES data.country(identidier)', 'description VARCHAR(50)', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION', 'dataset VARCHAR(10)'])
    print("measurement")
    create_table('data','measurement', ['city INTEGER REFERENCES data.city(identidier)', 'mark TIMESTAMP WITH TIME ZONE', 'temperature DOUBLE PRECISION'])
    print("coastline")
    create_table('data', 'coastline', ['shape INTEGER', 'segment INTEGER', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION'])

    print("Импорт данных в таблицы")
    print("Директория -> /home/vm/lab03/lab03.1/lab3/testfolder/data/region.csv")
    import_csv_to_table('data', '/home/vm/lab03/lab03.1/lab3/testfolder/data/region.csv', 'region')
    print("Директория -> /home/vm/lab03/lab03.1/lab3/testfolder/data/country.csv")
    import_csv_to_table('data', '/home/vm/lab03/lab03.1/lab3/testfolder/data/country.csv', 'country')
    print("Директория -> /home/vm/lab03/lab03.1/lab3/testfolder/data/cities.csv")
    import_csv_to_table('data', '/home/vm/lab03/lab03.1/lab3/testfolder/data/city.csv', 'city')
    print("Директория -> /home/vm/lab03/lab03.1/lab3/testfolder/coastline/CSV/ne_10m_coastline.csv")
    import_csv_to_table('data', '/home/vm/lab03/lab03.1/lab3/testfolder/coastline/CSV/ne_10m_coastline.csv', 'coastline')
    print("!Работа завершена!")
    conn.close()
else:
    print("Ошибка подключения! Убедитесь, что вы правильно выбрали IP-адрес")
