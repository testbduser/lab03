
import os
import shapefile
import csv
import json
import pandas as pd

print("COASTLINE FILES")

def shp_to_json(input_shp, output_json):
    reader = shapefile.Reader(input_shp)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    data = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        data.append({"properties": atr, "geometry": geom})

    with open(output_json, 'w') as json_file:
        json.dump(data, json_file)

    print(f"Сконвертировано: {input_shp}")

def json_to_csv(input_json, output_csv):
    with open(input_json, 'r') as json_file:
        data = json.load(json_file)

    with open(output_csv, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)

        # Записываем заголовок CSV
        header = ["shape", "segment","latitude", "longitude"]
        writer.writerow(header)

        # Записываем строки данных с дополнительными значениями столбцов
        shape_value = 0
        segment_value = 0
        for item in data:
            coordinates = item["geometry"]["coordinates"]
            for coord in coordinates:
                writer.writerow([shape_value, segment_value, coord[1], coord[0]])
                segment_value += 1
            shape_value += 1

folder = "coastline"

input_folder = "coastline"
output_folder = os.path.join(input_folder, "CSV")

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

shp_files = [file for file in os.listdir(input_folder) if file.lower().endswith(".shp")]

total_files = len(shp_files)
processed_files = 0

for shp_file in shp_files:
    processed_files += 1
    print(f"Конвертация файла ->  {shp_file}") 

    input_shp = os.path.join(input_folder, shp_file)
    output_json = os.path.join(output_folder, f"{os.path.splitext(shp_file)[0]}.json")
    output_csv = os.path.join(output_folder, f"{os.path.splitext(shp_file)[0]}.csv")

    shp_to_json(input_shp, output_json)
    json_to_csv(output_json, output_csv)

print("<<Конвертация завершена>>")  

print("POPULATED_PLACES FILES")

def shp_to_json(input_shp, output_json):
    reader = shapefile.Reader(input_shp)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    data = []
    for sr in reader.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        data.append({"properties": atr, "geometry": geom})

    with open(output_json, 'w') as json_file:
        json.dump(data, json_file, indent=4)

    print(f"Сконвертировано: {input_shp}") 

def json_to_csv(input_json, output_csv):
    with open(input_json, 'r') as json_file:
        data = json.load(json_file)

    with open(output_csv, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)

        header = ["NAME", "latitude", "longitude"]

        header += ["shape", "segment"]
        writer.writerow(header)

        shape_value = 0
        segment_value = 0
        for item in data:
            properties = item["properties"]
            geometry = item["geometry"]
            writer.writerow([properties["NAME"], geometry["coordinates"][1], geometry["coordinates"][0], shape_value, segment_value])
            segment_value += 1
            shape_value += 1

folder = "populated_places"

input_folder = "populated_places"
output_folder = "populated_places/CSV"

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

shp_files = [file for file in os.listdir(input_folder) if file.lower().endswith(".shp")]

total_files = len(shp_files)
processed_files = 0

for shp_file in shp_files:
    processed_files += 1
    print(f"Конвиртируется в файл -> : {shp_file}")

    input_shp = os.path.join(input_folder, shp_file)
    output_json = os.path.join(output_folder, f"{os.path.splitext(shp_file)[0]}.json")
    output_csv = os.path.join(output_folder, f"{os.path.splitext(shp_file)[0]}.csv")

    shp_to_json(input_shp, output_json)
    json_to_csv(output_json, output_csv)

print("<<Конвертация  завершена>>")
