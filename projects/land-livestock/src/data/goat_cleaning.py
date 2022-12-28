from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")


male = pd.read_csv(Path.joinpath(interim_data_path, "goat_male.csv"))
# District-wise male goat population in 2019

female = pd.read_csv(Path.joinpath(interim_data_path, "goat_female.csv"))
# District-wise female goat population in 2019

male["female"] = "0"
female["female"] = "1"


female["state_name"] = male["state_name"]
female["district_name"] = male["district_name"]
male["total"] = female["total"]

df = pd.concat([male, female], axis=0)

cols = df.columns.tolist()

cols = [
    "state_name",
    "district_name",
    "female",
    "under_one_year",
    "one_year_and_above",
    "in_milk",
    "dry",
    "not_calved_once",
    "total_male",
    "total_female",
    "total",
]

df = df[cols]

df1 = df.sort_values(by="state_name")

df1.to_csv(Path.joinpath(processed_data_path, "goat.csv"), index=False)
