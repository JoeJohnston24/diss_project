import os
import re
import json
from datetime import datetime
from tqdm import tqdm

def organize_posts(data):
    pattern = re.compile(r'Date: ([^\r\n]+)(.*?)(?=(Date:|$))', re.DOTALL)
    matches = pattern.findall(data)

    posts = []

    for match in matches:
        if len(match) >= 2:
            date_str, post_content = match[0], match[1]
            posts.append((date_str, post_content.strip()))

    return posts

def read_mbox_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()
    else:
        print(f"The file '{file_path}' does not exist.")
        return None

def write_output_to_file(output, output_file_path):
    with open(output_file_path, "w", encoding="utf-8") as file:
        output_list = [{"date": date_str, "comment": post_content} for date_str, post_content in output]
        json.dump(output_list, file, indent=2)

def process_folder(mbox_folder, output_folder):
    for file_name in os.listdir(mbox_folder):
        if file_name.endswith(".mbox"):
            file_path = os.path.join(mbox_folder, file_name)
            try:
                mbox_data = read_mbox_file(file_path)
                if mbox_data:
                    posts = organize_posts(mbox_data)
                    if posts:
                        output_file_path = os.path.join(output_folder, f"posts_{file_name}.json")
                        write_output_to_file(posts, output_file_path)
                        print(f"Output file '{output_file_path}' created with {len(posts)} posts.")
            except Exception as e:
                print(f"An error occurred while processing {file_path}: {e}")

# Folder paths
mbox_folder = "/home/joe/Desktop/diss_project/dataset/usenet_data/sort"
output_folder = "/home/joe/Desktop/diss_project/dataset/usenet_data/extracted"

# Process mbox files in the specified folder with progress bar
print("Processing mbox files:")
process_folder(mbox_folder, output_folder)
print("Extraction completed.")
