import re
import json
from tqdm import tqdm

# Define regular expressions for objective and subjective patterns
objective_pattern = r'\bthe\b\s*(?:\w+\s+)*truth\b|the\s+(?:absolute|undeniable|incontrovertible)\s+truth\s+(?:is|that)'
subjective_pattern = r'\b(my|his|her|their|our|personal|subjective)\b\s+(?:\w+\s+)*truth\b|\b(?:believes|convinced|feels?)\b\s+(?:the\s+)?(?:\w+\s+)*truth\b|the\s+truth\s+(?:of\s+my\s+experience|I\s+feel|according\s+to\s+my\s+understanding)'

def analyze_comment(comment):
    objective_patterns_matched = []
    subjective_patterns_matched = []
    
    # Search for objective patterns
    objective_matches = list(re.finditer(objective_pattern, comment, flags=re.IGNORECASE))
    for match in objective_matches:
        objective_patterns_matched.append(match.group(0))
    
    # Remove objective matches from the comment to avoid overlap
    objective_free_comment = re.sub(objective_pattern, '', comment, flags=re.IGNORECASE)
    
    # Search for subjective patterns in the modified comment
    subjective_matches = list(re.finditer(subjective_pattern, objective_free_comment, flags=re.IGNORECASE))
    for match in subjective_matches:
        subjective_patterns_matched.append(match.group(0))
    
    return objective_patterns_matched, subjective_patterns_matched

# Function to process input JSON and generate output JSON
def process_json(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    results = []
    
    for item in tqdm(data, desc='Processing comments', unit=' comments'):
        date = item['date']
        comment = item.get('comment', '').strip()
        
        # Skip if comment is null or empty
        if not comment:
            continue
        
        # Analyze the comment
        objective_patterns_matched, subjective_patterns_matched = analyze_comment(comment)
        
        # Skip if no patterns matched
        if not objective_patterns_matched and not subjective_patterns_matched:
            continue
        
        result = {
            'date': date,
            'comment': comment,
            'objective_patterns': objective_patterns_matched if objective_patterns_matched else None,
            'subjective_patterns': subjective_patterns_matched if subjective_patterns_matched else None,
            'has_objective': bool(objective_patterns_matched),
            'has_subjective': bool(subjective_patterns_matched)
        }
        
        results.append(result)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)

# Example usage:
if __name__ == '__main__':
    input_file = '/home/joe/diss_project/cleaned_NeutralPolitics_comments_extracted.json'
    output_file = 'output_data.json'
    
    process_json(input_file, output_file)
    print(f"Analysis complete. Results saved to {output_file}.")
