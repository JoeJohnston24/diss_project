import os
import json
import re
import nltk
import contractions
from nltk.corpus import words
from tqdm import tqdm
from nltk.tokenize import word_tokenize
from dateutil import parser as date_parser
from dateutil import tz
import database
import models
import datetime
import multiprocessing

nltk.download('words')
english_words = set(words.words())

class DatasetCleaner:
    def __init__(self, session, model, batch_size=1000):
        self.session = session
        self.model = model
        self.batch_size = batch_size

    def clean_dataset_multiprocessing(self, filename, last_processed_index=0):
        forum_name = self._extract_forum_name(filename)
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
            cleaned_data = self._clean_json_data(data, forum_name, last_processed_index)
            non_empty_comments = [entry for entry in cleaned_data if 'comment' in entry and entry['comment'].strip() != '']
            self._save_to_database_multiprocessing(non_empty_comments, forum_name, last_processed_index)

    def _extract_forum_name(self, filename):
        forum_name = os.path.basename(filename).split('_')[1]
        return forum_name

    def _clean_json_data(self, data, forum_name, last_processed_index):
        cleaned_data = []
        for i, entry in enumerate(tqdm(data, desc=f"Cleaning {forum_name} comments", unit="comment")):
            if i < last_processed_index:
                continue
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

    def _save_to_database_multiprocessing(self, cleaned_data, forum_name, start_index):
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = []

            iterator = pool.imap(process_comment, (entry['comment'] for entry in cleaned_data))

            with tqdm(total=len(cleaned_data), desc='Processing comments', unit=' comments') as pbar:
                for result in iterator:
                    if result:
                        results.append(result)
                    pbar.update(1)

            batch = []
            for i, entry in enumerate(cleaned_data):
                index = start_index + i
                date_str = entry.get('date')
                if date_str:
                    try:
                        post_date = date_parser.parse(date_str)
                        if post_date.utcoffset() is not None:
                            # Check if timezone offset is within supported range
                            offset_hours = post_date.utcoffset().total_seconds() / 3600
                            if abs(offset_hours) > 14:
                                print(f"Invalid timezone offset in date '{date_str}', normalizing to UTC")
                                post_date = post_date.astimezone(tz.UTC)
                    except (ValueError, OverflowError) as e:
                        print(f"Failed to parse date '{date_str}': {e}, using current datetime instead")
                        post_date = datetime.datetime.now(tz.UTC)
                else:
                    post_date = datetime.datetime.now(tz.UTC)

                record = self.model(
                    post_date=post_date,
                    comment=entry.get('comment'),
                    forum_name=forum_name
                )
                batch.append(record)

                if len(batch) >= self.batch_size:
                    self.session.bulk_save_objects(batch)
                    self.session.commit()
                    print(f"Committed batch of {len(batch)} records to the database.")
                    self._save_checkpoint(forum_name, index)
                    batch.clear()

            if batch:
                self.session.bulk_save_objects(batch)
                self.session.commit()
                print(f"Committed final batch of {len(batch)} records to the database.")
                self._save_checkpoint(forum_name, start_index + len(cleaned_data) - 1)

    def _save_checkpoint(self, forum_name, index):
        checkpoint_file = f"{forum_name}_checkpoint.txt"
        with open(checkpoint_file, 'w') as f:
            f.write(str(index))

    def _load_checkpoint(self, forum_name):
        checkpoint_file = f"{forum_name}_checkpoint.txt"
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return 0

def process_file(filename, session, model):
    cleaner = DatasetCleaner(session, model)
    forum_name = cleaner._extract_forum_name(filename)
    last_processed_index = cleaner._load_checkpoint(forum_name)
    cleaner.clean_dataset_multiprocessing(filename, last_processed_index)

def get_processed_files(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(f.read().splitlines())
    return set()

def save_processed_file(checkpoint_file, filename):
    with open(checkpoint_file, 'a') as f:
        f.write(filename + '\n')

def main():
    input_folder_path = os.getcwd() + '/data/usenet/extracted'
    json_files = [os.path.join(input_folder_path, f) for f in os.listdir(input_folder_path) if f.endswith(".json")]

    checkpoint_file = 'processed_files.txt'
    processed_files = get_processed_files(checkpoint_file)

    print(f"Found {len(json_files)} JSON files to process.")
    
    session = database.create_session()

    try:
        for filename in tqdm(json_files, desc="Processing files", unit="file"):
            if filename not in processed_files:
                print(f"Processing file: {filename}")
                process_file(filename, session, models.Usenet)
                save_processed_file(checkpoint_file, filename)
                print(f"Finished processing file: {filename}")
    finally:
        database.close_session(session)

    print("Data cleaning complete. Cleaned data is saved in the database.")

if __name__ == "__main__":
    main()
