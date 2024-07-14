import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Hold data
target_file = "transformed_data.csv"
log_file = "log_file.txt"

# 1 - Extract 
def extract_csv(file_process):
   dataframe = pd.read_csv(file_process)
   
   return dataframe


def extract_json(file_process):
   dataframe = pd.read_json(file_process, lines=True)
   
   return dataframe


def extract_xml(file_process):
   dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
   tree = ET.parse(file_process)
   root = tree.getroot()
   for row in root:
      car_model = row.find("car_model").text
      year_of_manufacture = row.find("year_of_manufacture").text
      price = float(row.find("price").text)
      fuel = row.find("fuel").text
      new_row = pd.DataFrame([{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])
      dataframe = pd.concat([dataframe, new_row], ignore_index=True)
      
      return dataframe


def extract():
   extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
   
   for csv_file in glob.glob("*.csv"):
      extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_csv(csv_file))], ignore_index=True)
      
   for json_file in glob.glob("*.json"):
      extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_json(json_file))], ignore_index=True)
      
   for xml_file in glob.glob("*.xml"):
      extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_xml(xml_file))], ignore_index=True)
      
   return extracted_data


 # 2 - Transformation
 
def transform(root):
   root["price"] = round(root.price, 2)
   
   return root


# 3 - Loading and Logging
def data_load(target_file, transformed_data):
   transformed_data.to_csv(target_file)


def log_progress(message):
   timestamp_format = "%Y-%h-%d-%H:-%M:-%S"
   now = datetime.now()
   timestamp = now.strftime(timestamp_format)
   with open(log_file, "a") as file:
      file.write(timestamp + "," + message + "\n")
      
      
log_progress("ETL Start")
log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Enden")
log_progress("Transform Started")
transformed_data = transform(extracted_data)
print("Transformed Data:")
print(transformed_data)
log_progress("Load phase Started")
data_load(target_file, transformed_data)
log_progress("Load phase Ended")
log_progress("ETL End")
