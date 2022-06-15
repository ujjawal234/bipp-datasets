# import os

import fuzzymatcher

# import numpy as np
import pandas as pd

# from fuzzywuzzy import fuzz, process

lgd_mapping_df = pd.read_csv(
    r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\external\LGD_CODE.csv"
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
    "state_name",
    "district_code",
    "district_name",
    "block_code",
    "block_name",
]
lgd_mapping_df["state_name"] = lgd_mapping_df["state_name"].str.strip().str.lower()
lgd_mapping_df["district_name"] = (
    lgd_mapping_df["district_name"].str.strip().str.strip()
)
lgd_mapping_df["block_name"] = lgd_mapping_df["block_name"].str.strip().str.upper()
lgd_mapping_df["uqid1"] = (
    lgd_mapping_df["state_name"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["district_name"].str.strip().str.lower()
)  # +"_"+lgd_mapping_df['block_name'].str.strip().str.lower()
lgd_mapping_df["uqid2"] = (
    lgd_mapping_df["state_name"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["district_name"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["block_name"].str.strip().str.lower()
)
lgd_mapping_df["id"] = lgd_mapping_df.index

map_df = pd.read_csv(
    r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\processed\merged data\merged_data.csv"
)
map_df.rename(
    columns={
        "State Name": "state_name",
        "District": "district_name",
        "Block": "block_name",
    },
    inplace=True,
)
map_df["uqid1"] = (
    map_df["state_name"].str.strip().str.lower()
    + "_"
    + map_df["district_name"].str.strip().str.lower()
)  # +"_"+map_df['block_name'].str.strip().str.lower()
map_df["uqid2"] = (
    map_df["state_name"].str.strip().str.lower()
    + "_"
    + map_df["district_name"].str.strip().str.lower()
    + "_"
    + map_df["block_name"].str.strip().str.lower()
)
map_df["id"] = map_df.index
matched_results = fuzzymatcher.fuzzy_left_join(
    map_df,
    lgd_mapping_df,
    ["uqid1", "uqid2"],
    ["uqid1", "uqid2"],
    left_id_col="id",
    right_id_col="id",
)
# df=pd.merge(left=map_df,right=lgd_mapping_df,how='left',left_on=['uqid','state_name','district_name','block_name'],right_on=['uqid','state_name','district_name','block_name'])

print(matched_results.columns)
matched_results.to_csv("matched1234_lgd_FUZZYLEFT.csv", index=False)
