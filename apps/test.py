import pyarrow.parquet as pq

# Load Parquet file and print metadata
parquet_file = pq.ParquetFile('/home/joe/reddit_dataset.parquet')
print(parquet_file.metadata)
