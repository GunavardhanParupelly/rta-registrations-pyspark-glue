import requests
import os
import re
from urllib.parse import unquote

api_url = "https://data.telangana.gov.in/api/1/metastore/schemas/dataset/items/d72e9ccc-de6a-47e6-96d3-fbc4cd233e73"

response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
else:
    print(f"error:{response.status_code}")

distribution_list = data["distribution"]

#print(distribution_list)

download_list = []
for d in distribution_list:
    #print(d)
    #print(d["downloadURL"])
    download_list.append(d["downloadURL"])

print(download_list)

destination_folder = "C:\\Users\\Gunav\\Desktop\\wedatasets"
os.makedirs(destination_folder, exist_ok=True)

for url in download_list:
    # Decode URL and extract date
    decoded_url = unquote(url)  # converts %20 to space, etc.
    
    match = re.search(r"(\d{2})-(\d{2})-(\d{4})\s+to", decoded_url)
    if match:
        day, month, year = match.groups()
        filename = f"transport_{year}-{month}.csv"
    else:
        filename = "transport_unknown.csv"

    filepath = os.path.join(destination_folder, filename)
    
    response = requests.get(url)
    with open(filepath, "wb") as f:
        f.write(response.content)
    
    print(f"Downloaded: {filename}")










