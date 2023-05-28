import paramiko
import psycopg2
import csv
import os
import time

# Информация о базе данных PostgreSQL
db_hostname = '192.168.122.33'
db_username = 'postgres'
db_password = 'password'
db_database = 'lab03'

# Информация для SSH-соединения
ssh_hostname = '192.168.122.33'
ssh_username = 'postgres'
ssh_password = 'password'

# Директория с измерениями
measurements_dir = '/home/postgres/convert_csv/output_csv/'

conn = None
ssh_client = None

def verify_database_info():
    # Проверка информации о базе данных
    global db_hostname, db_username, db_password, db_database
    
    print("Информация о базе данных PostgreSQL:")
    print(f"host -> {db_hostname}")
    print(f"user -> {db_username}")
    print(f"password -> {db_password}")
    print(f"database -> {db_database}")
    
    choice = input(" 1 - верная информация || 2 - неверная информация -> ")
    
    if choice == "2":
        db_hostname = input("host -> ")
        db_username = input("user -> ")
        db_password = input("password -> ")
        db_database = input("database -> ")

def create_scheme(scheme_name):
    # Создание схемы в базе данных
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {scheme_name};"
    cursor = conn.cursor()
    cursor.execute(create_schema_query)
    conn.commit()
    cursor.close()

def create_file_server(schema):
    # Создание файлового сервера (file_fdw) в базе данных
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

def create_foreign_table(table_name, csv_file):
    # Создание внешней таблицы на основе CSV-файла
    cursor = conn.cursor()
    create_scheme('external')
    cursor.execute(f"DROP FOREIGN TABLE IF EXISTS external.{table_name}")
    create_table_query = f"""
        CREATE FOREIGN TABLE external.{table_name} (
            city INTEGER,
            mark TIMESTAMP WITH TIME ZONE,
            temperature DOUBLE PRECISION
        )
        SERVER file_server
        OPTIONS (
            filename '{csv_file}',
            format 'csv',
            header 'true'
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

def create_measurement_foreign_tables():
    global ssh_client

    # Создание внешних таблиц для файлов измерений
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=ssh_hostname, username=ssh_username, password=ssh_password)

    sftp = ssh_client.open_sftp()
    measurement_files = [f for f in sftp.listdir(measurements_dir) if f.endswith('.csv')]
    sftp.close()

    for file_name in measurement_files:
        dataset = file_name.replace('.csv', '')

        table_name = f"measurement_{dataset}"

        csv_file = os.path.join(measurements_dir, file_name)

        create_foreign_table(table_name, csv_file)

    print("Созданы внешние таблицы для файлов измерений")

def create_table(schema, table_name, columns):
    # Создание таблицы в базе данных
    cursor = conn.cursor()
    drop_query = f"DROP TABLE IF EXISTS {schema}.{table_name.format()} CASCADE;"
    cursor.execute(drop_query)
    conn.commit()
    create_query = f"CREATE TABLE {schema}.{table_name} ({', '.join(columns)})"
    cursor.execute(create_query)
    conn.commit()
    cursor.close()

def import_csv_to_table(schema, csv_file, table_name):
    # Импорт данных из CSV-файла в таблицу
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

def get_csv_files():
    # Получение списка CSV-файлов на удаленном сервере
    csv_files = []
    
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=ssh_hostname, username=ssh_username, password=ssh_password)
    
    sftp = ssh_client.open_sftp()
    table_dir = '/home/postgres/convert_csv/output_csv/'
    files = sftp.listdir(table_dir)
    for file_name in files:
        if file_name.endswith('.csv'):
            csv_files.append(os.path.join(table_dir, file_name))
    sftp.close()
    ssh_client.close()
    
    return csv_files

def get_connection():
    # Установление соединения с базой данных
    global conn
    attempts = 0
    max_attempts = 5
    delay = 2
    
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host=db_hostname,
                database=db_database,
                user=db_username,
                password=db_password
            )
            print("<<Connected>>")
            break
        except psycopg2.Error as e:
            print("ERROR -> ", e)
            attempts += 1
            print(f"Restart after {delay} sec - ")
            time.sleep(delay)

def merge_all_scheme(schema, table_name):
    # Объединение всех внешних таблиц из схемы external в таблицу measurement схемы data
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='external';")
    external_tables = cur.fetchall()
    
    for table in external_tables:
        cur.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM external.{table[0]};")
    
    conn.commit()
    cur.close()
    conn.close()

verify_database_info()
get_connection()

if conn is not None:
    create_measurement_foreign_tables()
    print("Importing data into table:")
    print("Соединение всех внешних таблиц из схемы external в таблицу measurement схемы data")
    merge_all_scheme('data', 'measurement')

    conn.close()
else:
    print("Ошибка подключения!")
