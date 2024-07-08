import json
from tqdm import tqdm
import multiprocessing
import database
import models
import logging
import torch
from transformers import DistilBertTokenizer, DistilBertForSequenceClassification

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize DistilBERT for sentiment analysis with the correct pre-trained sentiment model
tokenizer = DistilBertTokenizer.from_pretrained('distilbert-base-uncased-finetuned-sst-2-english')
model = DistilBertForSequenceClassification.from_pretrained('distilbert-base-uncased-finetuned-sst-2-english')

# Ensure the model is in evaluation mode
model.eval()

def get_bert_sentiment_score(comment):
    """
    Get BERT sentiment score for a comment.
    """
    inputs = tokenizer(comment, return_tensors='pt', truncation=True, padding=True, max_length=512)
    logging.debug(f"Tokenized inputs: {inputs}")

    # Perform inference
    with torch.no_grad():
        outputs = model(**inputs)
        logits = outputs.logits
        sentiment_score = torch.nn.functional.softmax(logits, dim=-1)[0][1].item()  # Get the probability of the positive class
        logging.debug(f"Sentiment score for '{comment}': {sentiment_score}")

    return sentiment_score

def process_comment(item):
    """
    Process a single comment to extract sentiment score.
    """
    comment_text = item.comment.strip()
    if not item.has_detection or not comment_text:
        return None

    # Get the BERT sentiment score
    sentiment_score = get_bert_sentiment_score(comment_text)
    logging.debug(f"Processing comment: {comment_text} with sentiment score: {sentiment_score}")

    return {
        'id': item.id,
        'post_date': item.post_date,
        'comment': comment_text,
        'has_detection': True,
        'sentiment_score': sentiment_score  # Add BERT sentiment score
    }

def process_comments_multiprocessing(comments, num_cores=1):  # Temporarily use 1 core for debugging
    """
    Process comments using multiprocessing with a specified number of cores.
    """
    results = []
    
    with multiprocessing.Pool(processes=num_cores) as pool:
        results = pool.map(process_comment, comments)  # Use map instead of imap for simplicity
        
    return [result for result in results if result]  # Filter out None values

def fetch_comments(table_name, session, batch_size):
    """
    Fetch comments from the database.
    """
    if table_name == 'test':
        model = models.Test
    elif table_name == 'usenet':
        model = models.Usenet
    elif table_name == 'reddit':
        model = models.Reddit
    else:
        raise ValueError("Invalid table name. Choose 'test', 'usenet', or 'reddit'.")

    total_comments = session.query(model).filter_by(has_detection=True).count()
    logging.info(f"Total comments to process: {total_comments}")

    for offset in range(0, total_comments, batch_size):
        comments = session.query(model).filter_by(has_detection=True).offset(offset).limit(batch_size).all()
        logging.info(f"Fetched {len(comments)} comments from offset {offset}.")
        yield comments

def process_data_from_db(table_name, batch_size=100000):  # Adjust batch size for better handling
    """
    Fetch comments from the database, process them, and update the database.
    """
    session = database.create_session()

    try:
        for comments in fetch_comments(table_name, session, batch_size):
            logging.info(f"Processing batch of {len(comments)} comments.")

            if not comments:
                logging.info("No comments fetched for the current batch.")
                continue

            try:
                # Process comments using 1 core for debugging
                processed_data = process_comments_multiprocessing(comments, num_cores=1)
                logging.info(f"Processed {len(processed_data)} comments.")
            except Exception as e:
                logging.error(f"Error in processing comments: {str(e)}")
                continue

            for item in processed_data:
                record = session.query(models.Test).filter_by(id=item['id']).first()  # Adjust for correct table model
                if record:
                    record.has_detection = True
                    record.sentiment_score = item['sentiment_score']  # Update sentiment score
                    session.add(record)

            try:
                session.commit()
                logging.info(f"Committed changes for current batch.")
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
    table_to_analyse = input("Enter the table to analyse (test/usenet/reddit): ").strip().lower()
    process_data_from_db(table_to_analyse)
