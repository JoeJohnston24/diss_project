import matplotlib.pyplot as plt
import pandas as pd
import json  # Add this line
from models import Usenet
import database

# Create an engine and session
session = database.create_session()

# Query data from table
def query_data(table_class):
    """
    Query data from a table and return post dates, construct patterns, and comment counts.
    Args:
        table_class: The SQLAlchemy model class for the table to query.
    Returns:
        list of tuples: Each tuple contains (post_date, construct_patterns, comment_count).
    """
    try:
        results = session.query(
            table_class.post_date,
            table_class.construct_patterns
        ).filter(table_class.has_detection_cc == True).all()
        print(f"Query returned {len(results)} results.")
    except Exception as e:
        print(f"Error querying data: {e}")
        return []

    # Aggregate the counts
    data = {}
    for post_date, patterns in results:
        try:
            # Convert to naive datetime if it's timezone-aware
            if post_date.tzinfo is not None:
                post_date = post_date.astimezone(None)  # Convert to naive datetime

            if post_date not in data:
                data[post_date] = {'construct_patterns': 0, 'comment_count': 0}
            if patterns:
                data[post_date]['construct_patterns'] += len(json.loads(patterns))
            data[post_date]['comment_count'] += 1
        except Exception as e:
            print(f"Skipping invalid date: {post_date} due to error: {e}")

    # Convert to list of tuples
    return [(date, counts['construct_patterns'], counts['comment_count']) for date, counts in data.items()]

# Extract data from table
table_data = query_data(Usenet)

# Check if any data was retrieved
if not table_data:
    print("No data retrieved from the query. Exiting the script.")
    exit()

# Convert the results into DataFrames
def convert_to_dataframe(data, label):
    df = pd.DataFrame(data, columns=['post_date', 'construct_patterns', 'comment_count'])
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
    construct_patterns=('construct_patterns', 'sum'),
    comment_count=('comment_count', 'sum')
).reset_index()

# Merge with total comments DataFrame
yearly_df = yearly_df.merge(total_comments_df, on='year', how='left')

# Calculate the normalized ratios
yearly_df['ratio_per_comment'] = yearly_df['construct_patterns'] / yearly_df['total_comments']

# Save the aggregated DataFrame to a CSV file
yearly_df.to_csv('aggregated_construct_patterns_by_year.csv', index=False)
print("Aggregated DataFrame saved to 'aggregated_construct_patterns_by_year.csv'")

# Plotting the counts
fig, ax1 = plt.subplots(figsize=(14, 7))

ax1.set_xlabel('Year')
ax1.set_ylabel('Count', color='tab:blue')
ax1.plot(yearly_df['year'], yearly_df['construct_patterns'], marker='o', linestyle='-', label='Construct Patterns', color='blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.legend(loc='upper left')

plt.title('Construct Patterns by Year')
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig('construct_patterns_by_year_counts.png')
print("Plot saved to 'construct_patterns_by_year_counts.png'")

# Show the plot
plt.show()

# Plotting the ratios
fig, ax2 = plt.subplots(figsize=(14, 7))

ax2.set_xlabel('Year')
ax2.set_ylabel('Ratio per Comment', color='tab:green')
ax2.plot(yearly_df['year'], yearly_df['ratio_per_comment'], marker='s', linestyle='-', label='Ratio per Comment', color='green')
ax2.tick_params(axis='y', labelcolor='tab:green')
ax2.legend(loc='upper right')

plt.title('Ratios of Construct Patterns by Year')
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig('ratios_by_year.png')
print("Plot saved to 'ratios_by_year.png'")

# Show the plot
plt.show()

# Close the session and dispose of the engine
database.close_session(session)
