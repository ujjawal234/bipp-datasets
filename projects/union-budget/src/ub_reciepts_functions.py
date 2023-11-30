import pandas as pd
import re
from pathlib import Path
import csv
import numpy as np



def filter(df):
    
    df=df.dropna(axis=0, how='all').dropna(axis=1, how='all')
    col_df = df.loc[:, df.map(lambda x: isinstance(x, int)).any()].iloc[1:4,-5:]
    col_df.replace(pd.NA, '', inplace=True)
    irow = pd.concat([col_df.iloc[0:3].astype(str).agg(' '.join), col_df.iloc[3:]]).dropna(axis=0, how='all').dropna(axis=1, how='all')
    heads =df.loc[df.iloc[:,0].apply(lambda x: isinstance(x, int))]
    heads = heads.iloc[:, :2]
    heads.columns = ['key','head']
    heads= heads.reset_index(drop = True)
    heads_dict = dict(zip(heads['key'], heads['head']))
    sub_heads=df.loc[df.iloc[:,1].str.strip(".").str.contains(".", na=False, regex=False)].iloc[:,1:]
    sub_heads.columns = ['heads', 'subhead'] + irow.iloc[:, 0].tolist()
    sub_heads['heads'] = sub_heads['heads'].astype(str).str.split('.').str[0]
    sub_heads['heads'] = pd.to_numeric(sub_heads['heads'], errors='coerce')
    sub_heads['heads'] = sub_heads['heads'].map(heads_dict)
    sub_heads= sub_heads.reset_index(drop= True)
    sub_heads.columns=sub_heads.columns.str.strip()

    return sub_heads



def split_variable_column(df: pd.DataFrame, column_name: str = "variable"):
    df['year'] = df[column_name].str.extract(r'(\d{4}-\d{4})')
    df['estimate_type'] = df[column_name].str.replace(r'\d{4}-\d{4}', '', regex=True).str.strip()
    return df.drop(columns=column_name)


def data_cleaning_pipeline(df: pd.DataFrame):
    return df.pipe(filter).drop(columns='Major Head')

