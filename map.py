import psycopg2
import matplotlib.pyplot as plt

# Подключение к базе данных
conn = psycopg2.connect(host="192.168.122.33", database="lab03", user="postgres", password="password")
cur = conn.cursor()

# Выполнение запроса к таблице coastline
cur.execute("SELECT latitude, longitude FROM data.coastline")
rows = cur.fetchall()

# Закрытие соединения с базой данных
cur.close()
conn.close()

# Разделение координат на отдельные списки
latitude = [row[0] for row in rows]
longitude = [row[1] for row in rows]

# Запрос на ввод ширины точек
point_width = float(input("Введите ширину точек: "))

# Построение графика
plt.scatter(longitude, latitude, s=point_width)
plt.xlabel("longitude")
plt.ylabel("latitude")
plt.title("Распределение береговой линии")
plt.show()
