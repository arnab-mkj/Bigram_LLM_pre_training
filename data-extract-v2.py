import os
import pyarrow.parquet as pq
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def convert_parquet_to_csv(parquet_file, output_file, chunk_size=10000):
    # Open the Parquet file
    parquet_file = pq.ParquetFile(parquet_file)

    # Get the number of rows in the Parquet file using metadata
    num_rows = parquet_file.metadata.num_rows

    # Process the Parquet file in chunks
    with open(output_file, 'w', encoding='utf-8') as f:
        for start in range(0, num_rows, chunk_size):
            # Read a chunk of the Parquet file
            chunk = parquet_file.read_row_group(start // chunk_size)
            df = chunk.to_pandas()

            # Write the DataFrame to CSV
            df.to_csv(f, header=(start == 0), index=False)  # Write header only for the first chunk

def convert_parquet_files_in_directory(directory, output_directory):
    # Get all Parquet files in the directory
    parquet_files = [f for f in os.listdir(directory) if f.endswith('.parquet')]

    # Prepare output file paths
    output_files = [os.path.join(output_directory, f.replace('.parquet', '.csv')) for f in parquet_files]

    # Use ThreadPoolExecutor to parallelize the conversion
    with ThreadPoolExecutor() as executor:
        # Use tqdm to show progress
        list(tqdm(executor.map(lambda f: convert_parquet_to_csv(
            os.path.join(directory, f), 
            os.path.join(output_directory, f.replace('.parquet', '.csv'))
        ), parquet_files), total=len(parquet_files)))

# Example usage
input_directory = 'E:/progs/llm/openwebtext_en/data'
output_directory = 'E:/progs/llm/openwebtext_en/data'
convert_parquet_files_in_directory(input_directory, output_directory)