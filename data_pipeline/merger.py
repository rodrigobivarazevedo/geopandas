import os
import pandas as pd

# Directory containing the CSV files
directory = "processed_data/"

# Initialize an empty list to store DataFrames
dfs = []

# Iterate over each file in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        # Read the CSV file into a DataFrame
        filepath = os.path.join(directory, filename)
        df = pd.read_csv(filepath)
        dfs.append(df)

# Concatenate all DataFrames into a single DataFrame
merged_df = pd.concat(dfs, ignore_index=True)

# Output directory and filename for the merged CSV file
output_file = "combined_data/combined_farms_processed_files2.csv"

# Write the merged DataFrame to a new CSV file
merged_df.to_csv(output_file, index=False)

print("Merged CSV file saved as:", output_file)


