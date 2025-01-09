import os
import pandas as pd
from tqdm import tqdm

def csv_files_in_dir(directory):
    files = []
    for filename in os.listdir(directory):
        if filename.endswith(".csv") and os.path.isfile(os.path.join(directory, filename)):
            files.append(filename)
    return files

folder_path = "openwebtext_en/data"
output_file_train = "output_train.txt"
output_file_val = "output_val.txt"
vocab_file = "vocab.txt"

files = csv_files_in_dir(folder_path)
total_files = len(files)

split_index = int(total_files * 0.9)
files_train = files[:split_index]
files_val = files[split_index:]

vocab = set()

# Process training files
with open(output_file_train, "w", encoding="utf-8") as outfile:
    for filename in tqdm(files_train, total=len(files_train)):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)  # Read CSV file

        text = ' '.join(df['text'].astype(str).tolist())  # Assuming 'text' is the column name
        outfile.write(text + '\n')  # Write text to output file
        characters = set(text)
        vocab.update(characters)

# Process validation files
with open(output_file_val, "w", encoding="utf-8") as outfile:
    for filename in tqdm(files_val, total=len(files_val)):
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path)  # Read CSV file

        text = ' '.join(df['text'].astype(str).tolist())  # Assuming 'text' is the column name
        outfile.write(text + '\n')  # Write text to output file
        characters = set(text)
        vocab.update(characters)

# Write vocabulary to file
with open(vocab_file, "w", encoding="utf-8") as vfile:
    for char in vocab:
        vfile.write(char + '\n')