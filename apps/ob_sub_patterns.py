import spacy
import json
from tqdm import tqdm
import concurrent.futures
import database
import models

# Load spaCy English language model
nlp = spacy.load('en_core_web_sm')

# List of adjectives to look for in subjective patterns
ADJECTIVES = ['awful', 'neglected', 'important', 'significant', 'critical', 'essential', 
              'fundamental', 'basic', 'primary', 'true', 'false', 'real', 
              'apparent', 'objective', 'subjective', 'emotional', 'personal',
              'controversial', 'valid', 'invalid', 'logical', 'illogical', 
              'rational', 'irrational', 'reasonable', 'unreasonable', 
              'credible', 'incredible', 'convincing', 'unconvincing', 
              'biased', 'unbiased', 'persuasive', 'unpersuasive', 
              'coherent', 'incoherent', 'consistent', 'inconsistent', 
              'fair', 'unfair', 'just', 'unjust', 'clear', 'unclear', 
              'accurate', 'inaccurate', 'precise', 'imprecise', 
              'appropriate', 'inappropriate', 'reliable', 'unreliable', 
              'sensible', 'nonsensical', 'plausible', 'implausible', 
              'legitimate', 'illegitimate', 'trustworthy', 'doubtful', 
              'dubious', 'profound', 'shallow', 'deep', 'superficial', 
              'complex', 'simple', 'comprehensive', 'limited', 'acceptable', 
              'unacceptable']

# Function to analyze comment using spaCy for subjective patterns
def analyze_comment_spacy(comment):
    doc = nlp(comment)
    subjective_patterns_matched = []

    for token in doc:
        if token.lower_ == 'truth':
            if token.head.text.lower() in ['my', 'her', 'his', 'their', 'our', 'your']:
                subjective_patterns_matched.append(f"{token.head.text} {token.text}")
            elif token.head.text.lower() == 'the':
                if token.head.head.pos_ == 'ADJ' and token.head.head.text.lower() in ADJECTIVES:
                    subjective_patterns_matched.append(f"{token.head.head.text} {token.head.text} {token.text}")
                else:
                    subjective_patterns_matched.append(f"{token.head.text} {token.text}")

    return list(set(subjective_patterns_matched))

# Function to process each comment and return result
def process_comment(item):
    comment = item.comment.strip()
    has_subjective = item.has_subjective

    # Skip analysis if has_subjective is already True
    if has_subjective:
        return None

    # Skip if comment is null or empty
    if not comment:
        return None

    # Analyze the comment using spaCy
    subjective_patterns_matched = analyze_comment_spacy(comment)

    # Return result if subjective patterns matched
    if subjective_patterns_matched:
        return {
            'id': item.id,
            'comment': comment,
            'has_subjective': True,
            'subjective_patterns': subjective_patterns_matched
        }
    else:
        return None

# Function to process comments using multithreading with tqdm progress bar
def process_comments_multithreaded(comments):
    results = []
    num_comments = len(comments)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_comment, item) for item in comments]

        # Use tqdm as a context manager to track progress
        with tqdm(total=num_comments, desc='Processing comments', unit=' comments') as pbar:
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)
                pbar.update(1)  # Update tqdm progress bar

    return results

# Function to load comments from the database and process them
def process_data_from_db(table_name):
    session = database.create_session()

    try:
        if table_name == 'usenet':
            comments = session.query(models.Usenet).limit(250).all()
        elif table_name == 'reddit':
            comments = session.query(models.Reddit).limit(250).all()
        else:
            raise ValueError("Invalid table name. Choose 'usenet' or 'reddit'.")

        print(f"Processing {len(comments)} comments from {table_name} table.")

        # Process comments
        processed_data = process_comments_multithreaded(comments)

        # Save results back to the database
        for item in processed_data:
            if table_name == 'usenet':
                record = session.query(models.Usenet).filter_by(id=item['id']).first()
            elif table_name == 'reddit':
                record = session.query(models.Reddit).filter_by(id=item['id']).first()

            record.has_subjective = True  # Update has_subjective to True
            record.subjective_patterns = json.dumps(item['subjective_patterns'])
            session.add(record)

        session.commit()

    finally:
        database.close_session(session)

    print(f"Processing complete. Updated records in {table_name} table.")

# Example usage:
if __name__ == '__main__':
    table_to_analyze = input("Enter the table to analyze (usenet/reddit): ").strip().lower()
    process_data_from_db(table_to_analyze)
