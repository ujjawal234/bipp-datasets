import pandas as pd
import re
from pathlib import Path
import csv
import numpy as np

def extract_rows_with_data_points(input_df):
    # Drop rows with NaN values in columns 2 and above
    print(input_df)
    cleaned_df = input_df.dropna(subset=input_df.columns[1:], how='all')
    
    cleaned_df = cleaned_df.map(lambda x: np.nan if pd.isna(x) or x == '...' else x)
    
    cleaned_df = cleaned_df.dropna(subset=cleaned_df.columns[1:], how='all')
    cleaned_df = cleaned_df.iloc[1:,:]
    
    cleaned_df['head'] = cleaned_df.iloc[:, 0:4].fillna('').astype(str).agg(' '.join, axis=1)
    cleaned_df = cleaned_df.iloc[:,4:]
    
    columns = ['head'] + [col for col in cleaned_df.columns if col != 'head']
    cleaned_df = cleaned_df[columns]
    
    
    return cleaned_df


def assign_column_names(df: pd.DataFrame):
    df.columns = ['head'] + df.iloc[0, 1:].to_list()
    return df.iloc[1:].reset_index(drop=True)


def concat_first_three_rows(df):
    
    return df


def remove_utf8(text):
    if isinstance(text, str):
        # Use regular expression to remove non-UTF-8 characters
        return ''.join(char for char in text if char.isascii())
    else:
        return text  # Return non-string values as is


def remove_newlines(text):
    if isinstance(text, str):
        # Replace "\n" with an empty string
        return text.replace("\n", "")
    else:
        return text  # Return non-string values as is


def remove_characters_from_columns(df):
    for column in df.columns[1:]:
        df[column] = df[column].replace('[=\\-]', '', regex=True)
        df[column] = df[column].replace('.*\\.', '', regex=True)
    return df

def remove_brackets(text):
    if isinstance(text, str):
        # Use regular expression to remove content within both parentheses and square brackets
        return re.sub(r'\([^)]*\)|\[[^\]]*\]|=', '', text)

    else:
        return text  # Return non-string values as is




def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    df['estimate_type'] = df[column_name].str.replace(r'\d{4}-\d{4}', '', regex=True).str.strip()
    return df.drop(columns=column_name)


def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(extract_rows_with_data_points).map(remove_newlines).map(remove_brackets).map(remove_utf8)


