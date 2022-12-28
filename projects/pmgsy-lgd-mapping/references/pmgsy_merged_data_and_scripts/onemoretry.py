# import os

import fuzzymatcher
import numpy as np
import pandas as pd

# from fuzzywuzzy import fuzz, process
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
lgd_mapping_df["uqid1"] = (
    lgd_mapping_df["state_name_z"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["district_name_z"].str.strip().str.lower()
    + "_"
    + lgd_mapping_df["block_name_z"].str.strip().str.lower()
)
lgd_mapping_df["id"] = lgd_mapping_df.index
choices = lgd_mapping_df["uqid1"]

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
df0 = df[df.state_name_z.notnull()]
df1 = df[df.state_name_z.isnull()]
df1.drop(
    [
        "state_code",
        "state_name_z",
        "district_code",
        "district_name_z",
        "block_code",
        "block_name_z",
    ],
    axis=1,
    inplace=True,
)
querys = map_df["uqid1"] + "_" + map_df["id"].astype(str)
dic = {}
query = df1["uqid1"].unique().tolist()
dict = {}
dframe = []
choice = lgd_mapping_df["uqid1"].tolist()
# for q in query:
# t=process.extractOne(q, choice)
# dict[q]=choice.index(t[0])
# print(q)
print(dict)
# data_items=dict.items()
# data_list=list(data_items)
# df3=pd.DataFrame(data_list)
# print(df)
# df2=pd.merge(left=lgd_mapping_df,right=df3,how='left',left_on=['id'],right_on=1)
# print(df2)
# df2.to_csv('df2.csv')
# df3.to_csv('df3.csv')
df2 = pd.read_csv(r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\df2.csv")
df5 = fuzzymatcher.fuzzy_left_join(df1, df2, left_on=["uqid1"], right_on="0")
final = pd.concat([df0, df5])
final.set_index("id_x")
df5.to_csv("df51.csv")
final.to_csv("final_fuzzy.csv")


for m in querys:
    # print(m)
    m1 = m.split("_")
    z = m1[0] + "_" + m1[1] + "_" + m1[2]
    # print(df.iloc[int(m1[-1]),41])
    if df.iloc[int(m1[-1]), 41] is np.nan:
        key = df.iloc[int(m1[-1]), 38]
        if key in dict.keys():
            u = dict[key]
        else:
            t = process.extractOne(m, choices)
            print(t)
            dict[z] = t[2]
            u = t[2]
        print(u)
        # ldg=lgd_mapping_df.iloc[[t[2]]]
        # result=pd.concat([map_df.iloc[[m1[-1]]],lgd_mapping_df.iloc[[t[2]]]],axis=1,join='left')
        # pd.merge(left=map_df.iloc[[m1[-1]]],right=lgd_mapping_df.iloc[[t[2]]],how='left',left_on=['uqid1'],right_on=['uqid1'])
        df.iloc[int(m1[-1]), [40, 41, 42, 43, 44, 45, 46]] = lgd_mapping_df.iloc[int(u)]
        print(df.iloc[[int(m1[-1])]])
print(dic)
df.to_csv("complete_total.csv", index=False)


# frame=pd.concat(dframe, axis=0, ignore_index=True)
# df.to_csv('lgd_code2_usingifss.csv',index=False)
