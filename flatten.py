import os
import shutil
import glob
from datetime import datetime

def flatten_citibike_data(source_folder, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
    # Find all CSV files in the source folder and its subdirectories
    csv_files = glob.glob(os.path.join(source_folder, '**', '*.csv'), recursive=True)
    
    for csv_file in csv_files:
        # Get the filename from the full path
        filename = os.path.basename(csv_file)
        # Construct the destination path
        dest_path = os.path.join(dest_folder, filename)
        
        metadata_file = os.path.join(dest_folder, f"{filename}.metadata.txt")
        # check if metadata file already exists
        if os.path.exists(metadata_file):
            # Check if checksum matches
            with open(metadata_file, 'r') as meta:
                lines = meta.readlines()
                existing_checksum = lines[2].strip().split(': ')[1]
                current_checksum = str(os.path.getsize(csv_file))
                if existing_checksum == current_checksum:
                    print(f"Metadata for {filename} already exists and matches. Skipping.")
                    continue
                else:
                    print(f"Metadata for {filename} exists but checksum does not match. Updating.")
                    os.remove(metadata_file)  # Remove the old metadata file
                    # Continue to create new metadata file

        ingestion_datetime = os.path.getmtime(csv_file)  # Get the last modified time as
        checksum = os.path.getsize(csv_file)
        with open(metadata_file, 'w') as meta:
            meta.write(f"file_name: {filename}\n")
            meta.write(f"ingestion_datetime: {ingestion_datetime}\n")
            meta.write(f"checksum: {checksum}\n")

        
        shutil.move(csv_file, dest_path)
        print(f"Moved {csv_file} to {dest_path}")

def main():
    source_folder = 'data/citibike'
    dest_folder = f"data/bronze"
    print(f"Flattening data from {source_folder} to {dest_folder}")

    flatten_citibike_data(source_folder, dest_folder)
    print("Flattening complete.")


if __name__ == "__main__":
    main()
