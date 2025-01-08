import pandas as pd

# Read a Parquet file into a DataFrame
df = pd.read_parquet('E:/progs/llm/openwebtext_en/data/train-00000-of-00020-567acd907ed91a97.parquet')

# Display the first few rows of the DataFrame
print(df.head())