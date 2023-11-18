import pandas as pd
import os


def read_file(data):
    union_budget_data= pd.read_excel(data)
    return union_budget_data


folder_path = r'projects\union-budget\data\raw\budget_at_a_glance_raw_data'

# List all files in the folder
files = os.listdir(folder_path)

# Loop through each Excel file and perform some action (e.g., print the first few rows)
for file in files:
    file_path = os.path.join(folder_path, file)
    # Check if the file is an Excel file
    
    df = pd.DataFrame(pd.read_excel(file_path))
    
    # Drop rows where all values are NaN (blank)
    df_cleaned_rows = df.dropna(axis=0, how='all')
    # Drop columns where all values are NaN (blank)
    df_cleaned_columns = df_cleaned_rows.dropna(axis=1, how='all')
    print(df_cleaned_columns.head())
   
    

