import matplotlib.pyplot as plt
import pandas as pd
from models import Usenet
import database

# Create an engine and session
session = database.create_session()

# Query data from table
def query_data(table_class):
    """
    Query data from a table and return post dates, objective counts, possessive counts, and subjective counts.
    Args:
        table_class: The SQLAlchemy model class for the table to query.
    Returns:
        list of tuples: Each tuple contains (post_date, objective_count, subjective_count, possessive_count, comment_count).
    """
    try:
        results = session.query(
            table_class.post_date,
            table_class.objective_patterns,
            table_class.subjective_patterns,
            table_class.possessive_patterns
        ).filter(table_class.has_detection == True).all()
        print(f"Query returned {len(results)} results.")
    except Exception as e:
        print(f"Error querying data: {e}")
        return []

    # Aggregate the counts
    data = {}
    for post_date, subj, obj, poss in results:
        try:
            # Convert to naive datetime if it's timezone-aware
            if post_date.tzinfo is not None:
                post_date = post_date.astimezone(None)  # Convert to naive datetime

            if post_date not in data:
                data[post_date] = {'objective_count': 0, 'subjective_count': 0, 'possessive_count': 0, 'comment_count': 0}
            if obj:
                data[post_date]['objective_count'] += len(obj)
            if subj:
                data[post_date]['subjective_count'] += len(subj)
            if poss:
                data[post_date]['possessive_count'] += len(poss)
            data[post_date]['comment_count'] += 1
        except Exception as e:
            print(f"Skipping invalid date: {post_date} due to error: {e}")

    # Convert to list of tuples
    return [(date, counts['objective_count'], counts['subjective_count'], counts['possessive_count'], counts['comment_count']) for date, counts in data.items()]

# Extract data from table change target
table_data = query_data(Usenet)

# Check if any data was retrieved
if not table_data:
    print("No data retrieved from the query. Exiting the script.")
    exit()

# Convert the results into DataFrames
def convert_to_dataframe(data, label):
    df = pd.DataFrame(data, columns=['post_date', 'objective_count', 'subjective_count', 'possessive_count', 'comment_count'])
    df['post_date'] = pd.to_datetime(df['post_date'], utc=True, errors='coerce')  # Coerce invalid dates to NaT
    df = df.dropna(subset=['post_date'])  # Drop rows where 'post_date' could not be parsed
    df['year'] = df['post_date'].dt.year  # Extract year for aggregation
    df['source'] = label
    return df

# Convert data to DataFrame 
test_df = convert_to_dataframe(table_data, 'Usenet')

# Query the total number of comments by year
def query_total_comments(table_class):
    try:
        results = session.query(
            table_class.post_date
        ).all()
        print(f"Total comments query returned {len(results)} results.")
    except Exception as e:
        print(f"Error querying total comments: {e}")
        return []

    # Aggregate the counts
    total_comments = {}
    for post_date, in results:
        try:
            # Convert to naive datetime if it's timezone-aware
            if post_date.tzinfo is not None:
                post_date = post_date.astimezone(None)  # Convert to naive datetime

            year = post_date.year
            if year not in total_comments:
                total_comments[year] = 0
            total_comments[year] += 1
        except Exception as e:
            print(f"Skipping invalid date: {post_date} due to error: {e}")

    # Convert to list of tuples
    return [(year, count) for year, count in total_comments.items()]

# Extract total comments data
total_comments_data = query_total_comments(Usenet)

# Convert total comments to DataFrame
total_comments_df = pd.DataFrame(total_comments_data, columns=['year', 'total_comments'])

# Combine all DataFrames
combined_df = pd.concat([test_df]) 

# Aggregate by year
yearly_df = combined_df.groupby(['source', 'year']).agg(
    objective_count=('objective_count', 'sum'),
    subjective_count=('subjective_count', 'sum'),
    possessive_count=('possessive_count', 'sum'),
    comment_count=('comment_count', 'sum')
).reset_index()

# Merge with total comments DataFrame
yearly_df = yearly_df.merge(total_comments_df, on='year', how='left')

# Calculate the normalized ratios
yearly_df['ratio_objective_per_comment'] = yearly_df['objective_count'] / yearly_df['total_comments']
yearly_df['ratio_subjective_per_comment'] = yearly_df['subjective_count'] / yearly_df['total_comments']
yearly_df['ratio_possessive_per_comment'] = yearly_df['possessive_count'] / yearly_df['total_comments']

# Save the aggregated DataFrame to a CSV file
yearly_df.to_csv('aggregated_patterns_by_year.csv', index=False)
print("Aggregated DataFrame saved to 'aggregated_patterns_by_year.csv'")

# Plotting the counts and the ratios
fig, ax1 = plt.subplots(figsize=(14, 7))

# Plotting the counts
ax1.set_xlabel('Year')
ax1.set_ylabel('Count', color='tab:blue')
ax1.plot(yearly_df['year'], yearly_df['objective_count'], marker='o', linestyle='-', label='Objective Patterns', color='blue')
ax1.plot(yearly_df['year'], yearly_df['subjective_count'], marker='^', linestyle='--', label='Subjective Patterns', color='orange')
ax1.plot(yearly_df['year'], yearly_df['possessive_count'], marker='x', linestyle=':', label='Possessive Patterns', color='red')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.legend(loc='upper left')

# Creating a second y-axis for the ratios
ax2 = ax1.twinx()
ax2.set_ylabel('Ratio per Comment', color='tab:green')
ax2.plot(yearly_df['year'], yearly_df['ratio_objective_per_comment'], marker='s', linestyle='-', label='Ratio (Objective / Comment)', color='green')
ax2.plot(yearly_df['year'], yearly_df['ratio_subjective_per_comment'], marker='D', linestyle='--', label='Ratio (Subjective / Comment)', color='purple')
ax2.plot(yearly_df['year'], yearly_df['ratio_possessive_per_comment'], marker='*', linestyle=':', label='Ratio (Possessive / Comment)', color='brown')
ax2.tick_params(axis='y', labelcolor='tab:green')
ax2.legend(loc='upper right')

plt.title('Patterns by Year with Ratios per Comment')
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig('patterns_by_year.png')
print("Plot saved to 'patterns_by_year.png'")

# Show the plot
plt.show()

# Close the session and dispose of the engine
database.close_session(session)
