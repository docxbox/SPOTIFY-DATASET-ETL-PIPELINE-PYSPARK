import os, sys
import requests
from zipfile import ZipFile
import json
from kaggle.api.kaggle_api_extended import KaggleApi


def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded Zip File to {filename}")
        return filename
    else:
        raise Exception(f"Failed to Download File: {response.status_code}")



def extract_zip_file(zip_filename, output_dir):
    with ZipFile(zip_filename, 'r') as zip_file:
        zip_file.extractall(output_dir)
        print(f"Extracted zip file to {output_dir}")
        print("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir):
    json_files = [f for f in os.listdir(output_dir) if f.endswith('.json')]
    if not json_files:
        raise FileNotFoundError("No JSON file found in directory.")
    file_path = os.path.join(output_dir, json_files[0])
    
    with open(file_path, 'r') as f:
        data = json.load(f)

    fixed_path = os.path.join(output_dir, "fixed_da.json")
    with open(fixed_path, "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
    
    print(f"Fixed JSON written to {fixed_path}")
    os.remove(file_path)

    
def download_kaggle_dataset(dataset, output_dir):
    api = KaggleApi()
    api.authenticate()
    os.makedirs(output_dir, exist_ok = True)
    api.dataset_download_files(dataset, path=output_dir, unzip=False)
    print(f"Downloaded dataset to {output_dir}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Extraction Path is Required")
        print("Example Usage:")
        print("python3 execute.py /home/Workspace/etl/extract/")
    else:
        try:
            print("Starting the extraction process")

            EXTRACT_PATH = sys.argv[1]
            dataset = "yamaerenay/spotify-dataset-19212020-600k-tracks"
            zip_filename = "spotify-dataset-19212020-600k-tracks.zip"

            download_kaggle_dataset(dataset, EXTRACT_PATH)

            zip_file_path = os.path.join(EXTRACT_PATH, zip_filename)
            extract_zip_file(zip_file_path, EXTRACT_PATH)

            fix_json_dict(EXTRACT_PATH)

        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit(1)



    