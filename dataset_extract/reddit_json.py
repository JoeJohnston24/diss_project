import json
import os 
from tqdm import tqdm
from json.decoder import JSONDecodeError

def extract_data_from_line(line):
    try:
        data = json.loads(line)
        extracted_data = {
            "created_utc": data["created_utc"],
            "body": data["body"]
        }
        return extracted_data
    except JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def extract_data_from_file(input_file, output_folder):
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file_name = f"{base_name}_extracted.json"
    output_file_path = os.path.join(output_folder, output_file_name)

    with open(input_file, "r", encoding="utf-8") as f:
        extracted_data_list = []
        for line in tqdm(f, desc=f"Processing {base_name}", unit="lines"):
            extracted_data = extract_data_from_line(line)
            if extracted_data:
                extracted_data_list.append(extracted_data)

    with open(output_file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data_list, f, indent=4)

if __name__ == "__main__":
    input_folder_path = os.getcwd() + 'dataset/reddit/raw'
    output_folder_path = os.getcwd() + 'dataset/reddit/cleaned'

    for file_name in os.listdir(input_folder_path):
        input_file_path = os.path.join(input_folder_path, file_name)
        extract_data_from_file(input_file_path, output_folder_path)