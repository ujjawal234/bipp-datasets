# import os

# import fuzzymatcher
# import numpy as np
import pandas as pd
from fuzzywuzzy import process

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
lgd_mapping_df["state_name"] = lgd_mapping_df["state_name"].str.strip().str.strip()
lgd_mapping_df["district_name"] = (
    lgd_mapping_df["district_name"].str.strip().str.strip()
)
lgd_mapping_df["block_name"] = lgd_mapping_df["block_name"].str.strip().str.upper()
lgd_mapping_df["uqid1"] = (
    lgd_mapping_df["state_name"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["district_name"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["block_name"].str.strip().str.lower()
)
lgd_mapping_df["id"] = lgd_mapping_df.index
choices = lgd_mapping_df["uqid1"]
# print(choices)
dframe = []
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
    + "_"
    + map_df["block_name"].str.strip().str.lower()
)
map_df["id"] = map_df.index
df = pd.merge(
    left=map_df,
    right=lgd_mapping_df,
    how="left",
    left_on=["uqid1"],
    right_on=["uqid1"],
)
# df.to_csv('fresh.csv', index=False)
# df1=df[df['state_name_y'=='']]
# print(df1)
query = map_df["uqid1"] + "_" + map_df["id"].astype(str)
# print(query)
for m in query:
    # print(m)
    m1 = m.split("_")
    z = m1[0] + "_" + m1[1] + "_" + m1[2]
    if z in choices.unique():
        result = pd.merge(
            left=map_df.iloc[[m1[-1]]],
            right=lgd_mapping_df,
            how="left",
            left_on=["uqid1"],
            right_on=["uqid1"],
        )
        print(m1)
        print(m)
    else:
        t = process.extractOne(m, choices)
        print(t)
        # ldg=lgd_mapping_df.iloc[[t[2]]]
        # result=pd.concat([map_df.iloc[[m1[-1]]],lgd_mapping_df.iloc[[t[2]]]],axis=1,join='left')
        result = pd.merge(
            left=map_df.iloc[[m1[-1]]],
            right=lgd_mapping_df.iloc[[t[2]]],
            how="left",
            left_on=["uqid1"],
            right_on=["uqid1"],
        )

    dframe.append(result)

    print(dframe)

frame = pd.concat(dframe, axis=0, ignore_index=True)
dframe.to_csv("lgd_code1_tech.csv", index=False)


# matched_results=fuzzymatcher.fuzzy_left_join(map_df, lgd_mapping_df,['uqid1'],['uqid1'],left_id_col='id', right_id_col='id')
# df=pd.merge(left=map_df,right=lgd_mapping_df,how='left',left_on=['uqid1'],right_on=['uqid1'])

# print( matched_results.columns)
# matched_results.to_csv('matched1234_lgdfuzzy1',index=False)
