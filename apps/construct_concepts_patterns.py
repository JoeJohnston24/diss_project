import spacy
import json
from tqdm import tqdm
import multiprocessing
import database
import models
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load the transformer-based spaCy English language model
nlp = spacy.load('en_core_web_sm')

patterns = { 
    "gender_identity_social_justice_keywords": [
        "gender fluidity", "non binary", "gender spectrum", "cisgender", 
        "intersectionality", "gender performativity", "socially constructed gender", 
        "gender nonconforming", "queer theory", "transgender", "gender expression"
    ],
    
    "race_power_identity_politics_keywords": [
        "white privilege", "systemic racism", "microaggressions", "cultural appropriation", 
        "anti racism", "oppressor vs oppressed", "critical race theory", "racial privilege", 
        "racial identity"
    ],
    
    "mental_health_pathologising_dissent_keywords": [
        "safe space", "trigger warning", "emotional labour", "trauma informed care", 
        "victimhood culture", "mental health fragility", "coddling", "intellectual freedom", 
        "emotional resilience", "disagreement as harm"
    ],
    
    "digital_identity_echo_chambers_keywords": [
        "digital identity", "echo chamber", "cancel culture", "virtue signaling", 
        "deplatforming", "algorithmic polarization", "online identity curation", 
        "filter bubble", "social media identity", "ideological echo"
    ]
}

def analyse_comment_spacy(comment):
    doc = nlp(comment)
    detected_patterns = set()
    text = doc.text.lower()

    # Check for patterns based on exact matches
    for pattern_category, keywords in patterns.items():
        for keyword in keywords:
            keyword_pattern = keyword.lower()
            if keyword_pattern in text:
                detected_patterns.add(keyword)
                print(f"Pattern Detected: {keyword}")  # Debugging print statement

    return list(detected_patterns)

def process_comment(item):
    comment_text = item.comment.strip()

    if not comment_text:
        return None

    detected_patterns = analyse_comment_spacy(comment_text)
    print(f"Detected patterns for comment {item.id}: {detected_patterns}")  # Debugging print statement

    if detected_patterns:
        return {
            'id': item.id,
            'post_date': item.post_date,
            'comment': comment_text,
            'construct_patterns': json.dumps(detected_patterns),
            'has_detection_cc': True  
        }
    else:
        return {
            'id': item.id,
            'post_date': item.post_date,
            'comment': comment_text,
            'construct_patterns': json.dumps([]), 
            'has_detection_cc': False  
        }

def process_comments_multiprocessing(comments):
    results = []

    num_cores = 10  # Use 10 CPU cores

    with multiprocessing.Pool(processes=num_cores) as pool:
        iterator = pool.imap(process_comment, comments)

        with tqdm(total=len(comments), desc='Processing comments', unit=' comments') as pbar:
            for result in iterator:
                if result:
                    results.append(result)
                pbar.update(1)

    return results

def process_data_from_db(table_name, batch_size=10000):  
    session = database.create_session()

    try:
        if table_name == 'test':
            model = models.Test
        elif table_name == 'usenet':
            model = models.Usenet
        elif table_name == 'reddit':
            model = models.Reddit
        else:
            raise ValueError("Invalid table name. Choose 'test', 'usenet', or 'reddit'.")

        total_comments = session.query(model).count()
        logging.info(f"Processing {total_comments} comments from {table_name} table.")

        for offset in range(0, total_comments, batch_size):
            comments = session.query(model).offset(offset).limit(batch_size).all()
            logging.info(f"Processing comments from offset {offset} to {offset + batch_size}")

            if not comments:
                logging.info("No comments fetched for the current batch.")
                continue

            try:
                processed_data = process_comments_multiprocessing(comments)
                logging.info(f"Processed {len(processed_data)} comments.")
            except Exception as e:
                logging.error(f"Error in processing comments: {str(e)}")
                continue

            for item in processed_data:
                record = session.query(model).filter_by(id=item['id']).first()
                if record:
                    print(f"Before update - ID: {record.id}, Patterns: {record.construct_patterns}")  # Debugging print statement
                    record.construct_patterns = item['construct_patterns']
                    record.has_detection_cc = item['has_detection_cc']
                    session.add(record)
                    print(f"After update - ID: {record.id}, Patterns: {record.construct_patterns}")  # Debugging print statement

            try:
                session.commit()
                logging.info(f"Committed changes for offset {offset}")

                # Verify the update
                for item in processed_data:
                    record = session.query(model).filter_by(id=item['id']).first()
                    print(f"Verified update - ID: {record.id}, Patterns: {record.construct_patterns}")  # Debugging print statement

            except Exception as e:
                logging.error(f"Error during commit: {str(e)}")
                session.rollback()
                continue

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")

    finally:
        database.close_session(session)

    logging.info(f"Processing complete. Updated records in {table_name} table.")

if __name__ == '__main__':
    table_name = 'reddit'  # Change this to 'usenet' or 'reddit' as needed
    process_data_from_db(table_name)
