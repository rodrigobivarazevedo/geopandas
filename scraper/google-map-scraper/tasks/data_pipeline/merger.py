import os
import pandas as pd

# Directory containing the CSV files
base_directory = "../../output/"

# Initialize an empty list to store DataFrames
dfs = []

# Iterate over each directory in the base directory
for top_directory in os.listdir(base_directory):
    # Check if the current directory is not "all"
    if top_directory != "all":
        top_directory_path = os.path.join(base_directory, top_directory)
        print(f"Checking top directory: {top_directory_path}")
        
        # Check if the current directory is a directory
        if os.path.isdir(top_directory_path):
            # Iterate over each subdirectory in the top directory
            for directory in os.listdir(top_directory_path):
                directory_path = os.path.join(top_directory_path, directory)
                #print(f"Checking subdirectory: {directory_path}")
                
                # Check if the current subdirectory is "csv"
                if directory == "csv" and os.path.isdir(directory_path):
                    #print(f"Entering csv directory: {directory_path}")
                    # Iterate over each file in the "csv" directory
                    for filename in os.listdir(directory_path):
                        if filename.endswith(".csv"):
                            # Read the CSV file into a DataFrame
                            filepath = os.path.join(directory_path, filename)
                            if os.path.getsize(filepath) != 0:
                                # read a json file
                                #df = pd.read_json(filepath)
                                df = pd.read_csv(filepath)
                                #print(f"Reading file: {filepath}")
                                # Append the DataFrame to the list
                                dfs.append(df)
                            else:
                                print(f"File is empty: {filepath}")

# Check the length of the list of DataFrames
print("Number of DataFrames:", len(dfs))

# Concatenate all DataFrames into a single DataFrame
merged_df = pd.concat(dfs, ignore_index=True)

# Output directory and filename for the merged CSV file
output_file = "merged_raw_files2.csv"

# Write the merged DataFrame to a new CSV file
merged_df.to_csv(output_file, index=False)

print("Merged CSV file saved as:", output_file)
