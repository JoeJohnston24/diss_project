import os
import json
import re
import nltk
import contractions
nltk.download('words')
from nltk.corpus import words
from tqdm import tqdm
from nltk.tokenize import word_tokenize

english_words = set(words.words())

class DatasetCleaner:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def clean_dataset(self):
        print(f"Cleaning dataset from {self.input_file} to {self.output_file}...")
        with open(self.input_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
            cleaned_data = self._clean_json_data(data)

        non_empty_comments = [entry for entry in cleaned_data if 'comment' in entry and entry['comment'].strip() != '']
        if non_empty_comments:
            with open(self.output_file, 'w', encoding='utf-8') as file:
                json.dump(non_empty_comments, file, indent=2)
        print("Dataset cleaning completed.")

    def _clean_json_data(self, data):
        print("Cleaning JSON data...")
        if isinstance(data, list):
            cleaned_data = []
            for entry in tqdm(data, desc="Cleaning comments", unit="comment"):
                if 'comment' in entry:
                    entry['comment'] = self._clean_comment(entry['comment'])
                cleaned_data.append(entry)
            print("JSON data cleaning completed.")
            return cleaned_data

    def _clean_comment(self, comment):
        # Apply contractions fix
        comment = contractions.fix(comment)
        
        # Define cleaning rules
        cleaning_rules = [
            # Remove URLs
            (r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ''),
            # Remove words with dots or hyphens in them
            (r'\b\w*\.\w+\b', ''),
            (r'\b\w*-\w+\b', ''),
            # Remove words with attached punctuation
            (r'\b\w*[\.,;\'"!?]+\w+\b', ''),
            # Remove words in all caps
            (r'\b[A-Z]+\b', ''),
            # Remove repeated spaces
            (r'\s+', ' '),
            # Remove minimal replies
            (r'^\s*Reply-To [^\w\s]+\s*$', ''),
            # Convert words to lowercase
            (r'\b([a-zA-Z]+)\b', lambda match: match.group(0).lower()),
            # Remove non-alphabetic characters
            (r'[^a-zA-Z\s\'-]', ''),
            # Remove email addresses
            (r'\S+@\S+', ''),
            # Remove Google Threads-related noise
            (r'X-Google-Thread:.*', ''),
            # Remove specific headers
            (r'(--|Xref|X-Google-ArrivalTime|-- PST|Reply-To|X-Antivirus|MIME-Version|X-Usenet-Provider|X-Face|X-No-Archive|X-Plain|X-It-Strategy|X-Antivirus-Status|VPS|logging-data|X-Abuse-and-DMCA-Info|Injection-|From|X-Google-Thread|X-Google-Attributes|X-Google-NewGroupId|X-Google-Language|Received|Path|From|Newsgroups:|alt.politics.communism|Subject|Organization|Lines|Message-ID|NNTP-Posting-Host|Mime-Version|X-Trace|X-Complaints-To|NNTP-Posting-Date|Complaints-To|Injection-Info|posting-account|User-Agent|Bytes|Content-Type|Content-Transfer-Encoding|References|X-Priority|X-MSMail-Priority|X-Newsreader|X-MimeOLE|NNTP-Posting-).*', ''),
        ]
        
        # Apply cleaning rules
        for pattern, replacement in cleaning_rules:
            comment = re.sub(pattern, replacement, comment)

        # Tokenize the comment using NLTK
        tokens = word_tokenize(comment)

        # Remove non-English words
        tokens = [word for word in tokens if self._is_english_word(word)]

        # Join tokens back to form the cleaned comment
        cleaned_comment = ' '.join(tokens)

        return cleaned_comment

    def _is_english_word(self, word):
        return word.lower() in english_words


def main():
    input_folder = "/home/joe/Desktop/diss_project/dataset/usenet_data/extracted"
    output_folder = "/home/joe/Desktop/diss_project/dataset/usenet_data/clean"

    json_files = [f for f in os.listdir(input_folder) if f.endswith(".json")]

    print(f"Found {len(json_files)} JSON files to process.")

    for filename in tqdm(json_files, desc="Processing files", unit="file"):
        input_file = os.path.join(input_folder, filename)
        output_file = os.path.join(output_folder, "clean_" + filename)
        clean_json_file(input_file, output_file)

    print("Data cleaning complete. Cleaned files are saved in the output directory.")

def clean_json_file(input_file, output_file):
    print(f"Cleaning JSON file: {input_file} -> {output_file}")
    cleaner = DatasetCleaner(input_file, output_file)
    cleaner.clean_dataset()

if __name__ == "__main__":
    main()
