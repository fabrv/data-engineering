# downlad files from 'https://s3.amazonaws.com/tripdata/[year]-citibike-tripdata.zip
# from the year 2013 to 2023

import os
import requests
from tqdm import tqdm
import zipfile

def download_file(url, dest_folder):
  if not os.path.exists(dest_folder):
    os.makedirs(dest_folder)  # Create the folder if it doesn't exist

  filename = os.path.join(dest_folder, url.split('/')[-1])
  
  # Check if the file already exists
  if os.path.exists(filename):
    print(f"File {filename} already exists. Skipping download.")
    return
  
  response = requests.get(url, stream=True)
  
  with open(filename, 'wb') as f:
    total_size = int(response.headers.get('content-length', 0))
    progress = tqdm(total=total_size, unit='iB', unit_scale=True)
    
    for data in response.iter_content(chunk_size=1024):
      progress.update(len(data))
      f.write(data)
    
    progress.close()
  
  print(f"Downloaded {filename}")

def unzip_file(zip_path, dest_folder):
  
  with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(dest_folder)
  
  print(f"Unzipped {zip_path} to {dest_folder}")

def main():
  base_url = 'https://s3.amazonaws.com/tripdata/'
  years = range(2013, 2024)  # From 2013 to 2023
  dest_folder = 'data/citibike'

  for year in years:
    url = f"{base_url}{year}-citibike-tripdata.zip"
    download_file(url, dest_folder)
    zip_path = os.path.join(dest_folder, f"{year}-citibike-tripdata.zip")
    unzip_file(zip_path, dest_folder)
if __name__ == "__main__":
  main()