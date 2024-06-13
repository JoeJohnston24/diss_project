import spacy
import json
from tqdm import tqdm
import concurrent.futures

# Load spaCy English language model
nlp = spacy.load('en_core_web_sm')

# Function to analyze comment using spaCy for subjective patterns
def analyze_comment_spacy(comment):
    doc = nlp(comment)
    subjective_patterns_matched = []

    for token in doc:
        # Check for subjective patterns
        if token.lower_ in ['my', 'his', 'her', 'their', 'our'] and token.head.text.lower() == 'truth':
            subjective_patterns_matched.append(token.head.text)
        elif token.lower_ in ['believe', 'believes', 'feel', 'feels'] and token.head.text.lower() == 'truth':
            subjective_patterns_matched.append(token.head.text)
        elif token.lower_ == 'truth' and token.head.text.lower() in ['of', 'is', 'according', 'to'] and token.head.head.text.lower() in ['the', 'my']:
            subjective_patterns_matched.append(token.head.head.text + ' ' + token.head.text)

    return list(set(subjective_patterns_matched))  # Ensure unique patterns

# Function to process each comment and return result
def process_comment(item):
    date = item['date']
    comment = item.get('comment', '').strip()

    # Skip if comment is null or empty
    if not comment:
        return None

    # Analyze the comment using spaCy
    subjective_patterns_matched = analyze_comment_spacy(comment)

    # Return result if subjective patterns matched
    if subjective_patterns_matched:
        return {
            'date': date,
            'comment': comment,
            'subjective_patterns': subjective_patterns_matched,
            'has_subjective': True  # Since subjective pattern matched
        }
    else:
        return None

# Function to process input JSON and generate output JSON using multithreading
def process_json_multithreaded(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    results = []
    num_comments = len(data)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_comment, item) for item in data]

        for future in tqdm(concurrent.futures.as_completed(futures), total=num_comments, desc='Processing comments', unit=' comments'):
            result = future.result()
            if result:
                results.append(result)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)

    print(f"Analysis complete. Results saved to {output_file}.")

# Example usage with multithreading:
if __name__ == '__main__':
    input_file = '/home/joe/diss_project/cleaned_lgbt_comments_extracted.json'
    output_file = 'output_data.json'

    process_json_multithreaded(input_file, output_file)
