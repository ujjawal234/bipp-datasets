import pandas as pd
import re
from pathlib import Path
import csv
import numpy as np

def extract_rows_with_data_points(df: pd.DataFrame):
    index = df.iloc[:,2:].dropna(how='all').index
    return df.iloc[index].reset_index(drop=True)




def filter_sub_heads(df):
    
    heads =df.loc[df.iloc[:,0].apply(lambda x: isinstance(x, int))]
    heads = heads.iloc[:, :2]
    heads.columns = ['key','head']
    heads= heads.reset_index(drop = True)
    heads_dict = dict(zip(heads['key'], heads['head']))
    sub_heads=df.loc[df.iloc[:,1].str.strip(".").str.contains(".", na=False, regex=False)].iloc[:,1:]
    sub_heads.columns = ['heads','subhead','','','','',''] 
    sub_heads['heads'] = sub_heads['heads'].astype(str).str.split('.').str[0]
    sub_heads['heads'] = pd.to_numeric(sub_heads['heads'], errors='coerce')
    sub_heads['heads'] = sub_heads['heads'].map(heads_dict)
    print(sub_heads)


    return sub_heads


def assign_column_names(df: pd.DataFrame):
    df.columns = ['head','subhead'] + df.iloc[0, 2:].to_list()
    return df.iloc[1:].reset_index(drop=True)


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
    return df.pipe(filter_sub_heads)


