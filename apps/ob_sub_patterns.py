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

# List of acceptable objective adjectives (no repeats)
acceptable_objective_adjectives = [
    "absolute", "reliable", "transparent", "genuine", "factual", "honest", "pure", 
    "authentic", "sincere", "steadfast", "stark", "raw", "brutal", "harsh", "cold", 
    "bleak", "bitter", "grim", "rigid", "severe", "unyielding", "inflexible", "sad",
    "resolute", "fixed", "stubborn", "unrelenting", "uncompromising", "rigorous", 
    "insistent", "adamant", "unwavering", "consistent", "direct", "simple", "clear",
    "plain", "straightforward", "forthright", "truthful", "valid", "verifiable",
    "unambiguous", "definitive", "strong", "unshakable", "concrete", "relevant", "pertinent"
]

# Define synonyms for "truth"
truth_synonyms = ["truth"]

def analyse_comment_spacy(comment):
    doc = nlp(comment)
    objective_patterns = set()
    possessive_patterns = set()
    subjective_patterns = set()

    for token in doc:
        # Debugging logs
        logging.debug(f"Token: {token.text}, POS: {token.pos_}, DEP: {token.dep_}")

        # Check for objective patterns
        if token.text.lower() in truth_synonyms:
            for left_token in token.lefts:
                if left_token.pos_ == 'ADJ' and left_token.text.lower() in acceptable_objective_adjectives:
                    pattern = f"the {left_token.text} {token.text}"
                    objective_patterns.add(pattern)
                    logging.debug(f"Objective Pattern Found: {pattern}")

        # Check for possessive patterns
        if token.dep_ in ['nsubj', 'dobj', 'attr'] and token.text.lower() in truth_synonyms:
            for neighbor in token.lefts:
                if neighbor.dep_ == 'poss' and neighbor.pos_ == 'PRON':
                    pattern = f"{neighbor.text} {token.text}"
                    possessive_patterns.add(pattern)
                    logging.debug(f"Possessive Pattern Found: {pattern}")

        # Check for subjective patterns
        if token.pos_ == 'ADJ' and token.text.lower() not in acceptable_objective_adjectives:
            if any(child.text.lower() in truth_synonyms for child in token.children):
                pattern = f"the {token.text} truth"
                subjective_patterns.add(pattern)
                logging.debug(f"Subjective Pattern Found: {pattern}")

    # Convert sets to lists and ensure no duplicates
    objective_patterns = list(objective_patterns)
    possessive_patterns = list(possessive_patterns)
    subjective_patterns = list(subjective_patterns)

    # Remove possessive patterns that are also objective patterns
    possessive_patterns = [p for p in possessive_patterns if p not in objective_patterns]

    return objective_patterns, possessive_patterns, subjective_patterns

def process_comment(item):
    # Extract the comment text and strip any leading/trailing whitespace
    comment_text = item.comment.strip()

    if item.has_detection:
        return None

    # Skip processing if the comment text is empty or None
    if not comment_text:
        return None

    # Analyse the comment text using the `analyse_comment_spacy` function
    objective_patterns, possessive_patterns, subjective_patterns = analyse_comment_spacy(comment_text)

    # Convert lists of patterns to JSON strings for `jsonb` columns
    objective_patterns_str = json.dumps(objective_patterns) if objective_patterns else '[]'
    subjective_patterns_str = json.dumps(subjective_patterns) if subjective_patterns else '[]'
    possessive_patterns_str = json.dumps(possessive_patterns) if possessive_patterns else '[]'

    # Prepare the result dictionary if any patterns are found
    if subjective_patterns or possessive_patterns:
        return {
            'id': item.id,
            'post_date': item.post_date,
            'comment': comment_text,
            'has_detection': True,  
            'objective_patterns': objective_patterns_str,  # Store JSON string
            'subjective_patterns': subjective_patterns_str,  # Store JSON string
            'possessive_patterns': possessive_patterns_str   # Store JSON string
        }
    else:
        return None

def process_comments_multiprocessing(comments):
    results = []

    # Use all available cores for multiprocessing
    num_cores = multiprocessing.cpu_count()

    with multiprocessing.Pool(processes=num_cores) as pool:
        # Correctly pass the list of comments to pool.imap
        iterator = pool.imap(process_comment, comments)

        with tqdm(total=len(comments), desc='Processing comments', unit=' comments') as pbar:
            for result in iterator:
                if result:
                    results.append(result)
                pbar.update(1)

    return results

def process_data_from_db(table_name, batch_size=100000):  # Adjust batch size for better handling
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

        total_comments = session.query(model).filter_by(has_detection=False).count()
        logging.info(f"Processing {total_comments} comments from {table_name} table.")

        for offset in range(0, total_comments, batch_size):
            comments = session.query(model).filter_by(has_detection=False).offset(offset).limit(batch_size).all()
            logging.info(f"Processing comments from offset {offset} to {offset + batch_size}")

            if not comments:
                logging.info("No comments fetched for the current batch.")
                continue

            try:
                # Pass the list of comments to `process_comments_multiprocessing`
                processed_data = process_comments_multiprocessing(comments) 
                logging.info(f"Processed {len(processed_data)} comments.")
            except Exception as e:
                logging.error(f"Error in processing comments: {str(e)}")
                continue

            for item in processed_data:
                record = session.query(model).filter_by(id=item['id']).first()
                if record:
                    record.has_detection = True
                    record.objective_patterns = item['objective_patterns']  
                    record.subjective_patterns = item['subjective_patterns']
                    record.possessive_patterns = item['possessive_patterns']
                    session.add(record)

            try:
                session.commit()
                logging.info(f"Committed changes for offset {offset}")
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

    # Testing the analyse_comment_spacy function
    def test_analyse_comment_spacy():
        test_comments = [
            # Test Case 1: Simple objective pattern
            {
                "comment": "The honest truth is that your truth matters.",
                "expected_objective_patterns": ["the honest truth"],
                "expected_possessive_patterns": ["your truth"],
                "expected_subjective_patterns": []
            },
            # Test Case 2: Simple possessive pattern
            {
                "comment": "Her truth is a reflection of her experiences.",
                "expected_objective_patterns": [],
                "expected_possessive_patterns": ["Her truth"],
                "expected_subjective_patterns": []
            },
            # Test Case 3: Adjective modifying "truth" (subjective pattern)
            {
                "comment": "Your truth can be subjective.",
                "expected_objective_patterns": [],
                "expected_possessive_patterns": ["Your truth"],
                "expected_subjective_patterns": []
            },
            # Test Case 4: Objective pattern with adjective
            {
                "comment": "The sad truth about the situation was evident.",
                "expected_objective_patterns": ["the sad truth"],
                "expected_possessive_patterns": [],
                "expected_subjective_patterns": []
            },
            # Test Case 5: Subjective pattern example
            {
                "comment": "The bitter truth is not always easy to accept.",
                "expected_objective_patterns": ["the bitter truth"],
                "expected_possessive_patterns": [],
                "expected_subjective_patterns": []
            },
            # Add more test cases as needed
        ]

        for case in test_comments:
            comment = case["comment"]
            objective_patterns, possessive_patterns, subjective_patterns = analyse_comment_spacy(comment)
            try:
                assert objective_patterns == case["expected_objective_patterns"], f"Failed for '{comment}' - Expected {case['expected_objective_patterns']} but got {objective_patterns}"
                assert possessive_patterns == case["expected_possessive_patterns"], f"Failed for '{comment}' - Expected {case['expected_possessive_patterns']} but got {possessive_patterns}"
                assert subjective_patterns == case["expected_subjective_patterns"], f"Failed for '{comment}' - Expected {case['expected_subjective_patterns']} but got {subjective_patterns}"
                print(f"Test passed for: '{comment}'")
            except AssertionError as e:
                print(e)

    # Run the test function
    # test_analyse_comment_spacy()
