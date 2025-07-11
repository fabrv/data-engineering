import os
import requests
from tqdm import tqdm
import zipfile
import sys
sys.path.append('/app/citibike_project')
from utils.slack_notifier import notify_failure, notify_success

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def download_file(url, dest_folder):
    print(f"Downloading {url} to {dest_folder}")
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
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
    metdataPath = zip_path.replace('.zip', '-metadata.txt')
    if os.path.exists(metdataPath):
        print(f"Metadata file {metdataPath} already exists. Skipping unzipping.")
        return
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(dest_folder)
    
    with open(metdataPath, 'w') as f:
        f.write(f"Downloaded from {zip_path}\n")
        f.write(f"Extracted to {dest_folder}\n")
    
    print(f"Unzipped {zip_path} to {dest_folder}")

def unzip_years(dest_folder):
    for year in range(2013, 2024):
        print("--------")
        print(f"Unzipping files for year {year}")
        for months in range(1, 13):
            zip_file = os.path.join(f"{dest_folder}/{year}-citibike-tripdata", f"{year}{months:02d}-citibike-tripdata.zip")
            print(f"Checking {zip_file}")
            if os.path.exists(zip_file):
                print(f"Unzipping {zip_file}")
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall(f"{dest_folder}/{year}-citibike-tripdata/{months:02d}")
                os.remove(zip_file)

def main():
    base_url = 'https://s3.amazonaws.com/tripdata/'
    years = range(2013, 2024)  # From 2013 to 2023
    dest_folder = 'data/citibike'
    
    for year in years:
        url = f"{base_url}{year}-citibike-tripdata.zip"
        download_file(url, dest_folder)
        zip_path = os.path.join(dest_folder, f"{year}-citibike-tripdata.zip")
        unzip_file(zip_path, dest_folder)
    
    unzip_years(dest_folder)

@data_loader
def load_data(*args, **kwargs):
    try:
        main()
        notify_success("Data Ingestion", {
            "files_processed": "CitiBike data 2013",
            "destination": "data/citibike"
        })
        return {"status": "completed", "message": "Descarga y extracción completada"}
    except Exception as e:
        notify_failure("Data Ingestion", str(e), {
            "step": "download_and_extract",
            "source": "s3.amazonaws.com/tripdata"
        })
        raise

if __name__ == "__main__":
    main()