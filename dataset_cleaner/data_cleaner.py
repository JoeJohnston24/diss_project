import os
import json
import string
import re
import nltk
import contractions
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from tqdm import tqdm
from datetime import datetime, timezone

nltk.download('punkt')
nltk.download('stopwords')

def clean_text(text):
    cleaning_rules = [
        (r"http[s]?://[^\s]+", ""),  # Remove URLs
        (r"\b\w*\.\w+\b", ""),  # Remove words with periods
        (r"\b\w*[-–—]+\w*\b", ""),  # Remove words with hyphens and dashes
        (r"\s+", " "),  # Remove repeated spaces
        (r"[^a-zA-Z\s]", ""),  # Keep only alphabetic characters and spaces
    ]
    
    # Apply cleaning rules
    for pattern, replacement in cleaning_rules:
        text = re.sub(pattern, replacement, text)
    
    text = text.lower()
    text = contractions.fix(text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    words = word_tokenize(text)
    return ' '.join(words)


def convert_utc_to_date(utc_timestamp):
    return datetime.fromtimestamp(int(utc_timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def process_file(input_file, output_dir):
    print(f"Processing file: {input_file}")
    with open(input_file, 'r') as file:
        data = json.load(file)

    cleaned_data = []
    seen_comments = set()

    for i, item in enumerate(tqdm(data, desc="Cleaning comments", unit="comment")):
        body = item.get('body', '')

        if body in ['[deleted]', '[removed]'] or body in seen_comments:
            continue

        cleaned_body = clean_text(body)

        if cleaned_body:
            date = convert_utc_to_date(item.get('created_utc', ''))
            cleaned_data.append({'date': date, 'comment': cleaned_body})
            seen_comments.add(body)

    filename = os.path.basename(input_file)
    output_file = os.path.join(output_dir, f"cleaned_{filename}")
    with open(output_file, 'w') as cleaned_file:
        json.dump(cleaned_data, cleaned_file, indent=4)

def main():
    input_dir = "/home/joe/Desktop/diss_project/dataset/reddit_data/extracted"
    output_dir = "/home/joe/Desktop/diss_project/dataset/reddit_data/clean"

    json_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".json")]

    print(f"Found {len(json_files)} JSON files to process.")

    for filename in tqdm(json_files, desc="Processing files", unit="file"):
        process_file(filename, output_dir)

    print("Data cleaning complete. Cleaned files are saved in the output directory.")

if __name__ == "__main__":
    main()

