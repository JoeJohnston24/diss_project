import os
import json
import re
import nltk
import contractions
from nltk.corpus import words
from tqdm import tqdm
from nltk.tokenize import word_tokenize
import database
import models

nltk.download('words')
english_words = set(words.words())

class DatasetCleaner:
    def __init__(self, session, model):
        self.session = session
        self.model = model

    def clean_dataset(self, data):
        cleaned_data = self._clean_json_data(data)
        non_empty_comments = [entry for entry in cleaned_data if 'comment' in entry and entry['comment'].strip() != '']
        self._save_to_database(non_empty_comments)

    def _clean_json_data(self, data):
        cleaned_data = []
        for entry in tqdm(data, desc="Cleaning comments", unit="comment"):
            if 'comment' in entry:
                entry['comment'] = self._clean_comment(entry['comment'])
            cleaned_data.append(entry)
        return cleaned_data

    def _clean_comment(self, comment):
        comment = contractions.fix(comment)
        cleaning_rules = [
            (r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ''),
            (r'\b\w*\.\w+\b', ''),
            (r'\b\w*-\w+\b', ''),
            (r'\b\w*[\.,;\'"!?]+\w+\b', ''),
            (r'\b[A-Z]+\b', ''),
            (r'\s+', ' '),
            (r'^\s*Reply-To [^\w\s]+\s*$', ''),
            (r'\b([a-zA-Z]+)\b', lambda match: match.group(0).lower()),
            (r'[^a-zA-Z\s\'-]', ''),
            (r'\S+@\S+', ''),
            (r'X-Google-Thread:.*', ''),
            (r'(--|Xref|X-Google-ArrivalTime|-- PST|Reply-To|X-Antivirus|MIME-Version|X-Usenet-Provider|X-Face|X-No-Archive|X-Plain|X-It-Strategy|X-Antivirus-Status|VPS|logging-data|X-Abuse-and-DMCA-Info|Injection-|From|X-Google-Thread|X-Google-Attributes|X-Google-NewGroupId|X-Google-Language|Received|Path|From|Newsgroups:|alt.politics.communism|Subject|Organization|Lines|Message-ID|NNTP-Posting-Host|Mime-Version|X-Trace|X-Complaints-To|NNTP-Posting-Date|Complaints-To|Injection-Info|posting-account|User-Agent|Bytes|Content-Type|Content-Transfer-Encoding|References|X-Priority|X-MSMail-Priority|X-Newsreader|X-MimeOLE|NNTP-Posting-).*', ''),
        ]
        
        for pattern, replacement in cleaning_rules:
            comment = re.sub(pattern, replacement, comment)

        tokens = word_tokenize(comment)
        tokens = [word for word in tokens if self._is_english_word(word)]
        cleaned_comment = ' '.join(tokens)

        return cleaned_comment

    def _is_english_word(self, word):
        return word.lower() in english_words

    def _save_to_database(self, cleaned_data):
        for entry in cleaned_data:
            record = self.model(
                post_date=entry.get('post_date'),
                comment=entry.get('comment')
            )
            self.session.add(record)
        self.session.commit()

def process_file(filename, session, model):
    with open(filename, 'r', encoding='utf-8') as file:
        data = json.load(file)
        cleaner = DatasetCleaner(session, model)
        cleaner.clean_dataset(data)

def main():
    input_folder_path = os.getcwd() + '/data/youtube/extracted'
    json_files = [os.path.join(input_folder_path, f) for f in os.listdir(input_folder_path) if f.endswith(".json")]

    print(f"Found {len(json_files)} JSON files to process.")

    session = database.create_session()

    try:
        for filename in tqdm(json_files, desc="Processing files", unit="file"):
            process_file(filename, session, models.Youtube)
    finally:
        database.close_session(session)

    print("Data cleaning complete. Cleaned data is saved in the database.")

if __name__ == "__main__":
    main()
