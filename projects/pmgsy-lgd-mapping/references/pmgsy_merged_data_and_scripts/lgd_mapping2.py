# import os
from pathlib import Path

# import fuzzymatcher
# import numpy as np
import pandas as pd
from fuzzywuzzy import process

# from pandas.io.parsers import read_csv

# Getting path directory
project_folder = str(Path(__file__).resolve().parents[2])
print(project_folder)
lgd_mapping_df = pd.read_csv(
    r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\external\LGD_CODE.csv"
)
map_df = pd.read_csv(
    r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\processed\merged data\merged_data.csv"
)

lgd_mapping_df = (
    lgd_mapping_df[
        [
            "St_LGD_code",
            "State Name(In English)",
            "Dt_LGD_code",
            "District Name(In English)",
            "Bk_LGD_code",
            "Block Name (In English) ",
        ]
    ]
    .drop_duplicates()
    .reset_index(drop=True)
)
lgd_mapping_df.columns = [
    "state_code",
    "state_name_z",
    "district_code",
    "district_name_z",
    "block_code",
    "block_name_z",
]
lgd_mapping_df["state_name_z"] = lgd_mapping_df["state_name_z"].str.strip().str.strip()
lgd_mapping_df["district_name_z"] = (
    lgd_mapping_df["district_name_z"].str.strip().str.strip()
)
lgd_mapping_df["block_name_z"] = lgd_mapping_df["block_name_z"].str.strip().str.upper()
lgd_mapping_df["uqid"] = (
    lgd_mapping_df["state_name_z"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["district_name_z"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["block_name_z"].str.strip().str.lower()
)
lgd_mapping_df["id"] = lgd_mapping_df.index
choices = lgd_mapping_df["uqid"]

map_df.rename(
    columns={
        "State Name": "state_name",
        "District": "district_name",
        "Block": "block_name",
    },
    inplace=True,
)
map_df["uqid"] = (
    map_df["state_name"].str.strip().str.lower()
    + "_"
    + map_df["district_name"].str.strip().str.lower()
    + "_"
    + map_df["block_name"].str.strip().str.lower()
)
map_df["id"] = map_df.index

df = pd.merge(
    left=map_df,
    right=lgd_mapping_df,
    how="left",
    left_on=["uqid"],
    right_on=["uqid"],
)
df1 = df[df.state_name_z.isnull()]

query = df1["uqid"].unique().tolist()
dict = {}
dframe = []
choice = lgd_mapping_df["uqid"].tolist()
# query=map_df['uqid'].unique().tolist()
# dict={}
for q in query:
    t = process.extractOne(q, choice)
    dict[q] = choice.index(t[0])
    print(q)
print(dict)
