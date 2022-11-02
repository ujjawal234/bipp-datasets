from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
interim_path = dir_path.joinpath("data/interim")

file = interim_path.joinpath("sep_2020_to_aug_2021.csv")


df = pd.read_csv(file)

id_list = list(df["hh_id"].unique())
id_list = id_list[0:100]

# print(id_list)

df1 = df[df["state"] == "Meghalaya"]

# df1=df[df["hh_id"].isin(id_list)]

print(len(df1))

df1.to_csv(interim_path.joinpath("mlrf_cmie_data_sample.csv"), index=False)
