from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")

df = pd.read_csv(Path.joinpath(interim_data_path, "cattle.csv"))
# District-wise cattle population in 2019

male = df[
    [
        "state_name",
        "district_name",
        "breed_type_name",
        "upto_one_and_half_years_male",
        "used_for_breeding_only_male",
        "used_for_agriculture_only_male",
        "agriculture_and_breeding_male",
        "bullock_cart_farm_operations_male",
        "others_male",
        "total_male",
        "total",
    ]
]
female = df[
    [
        "state_name",
        "district_name",
        "breed_type_name",
        "under_one_ year_female",
        "one_to_two_and_half_years_female",
        "in_milk_female",
        "dry_female",
        "not_calved_once_female",
        "others_female",
        "total_female",
        "total",
    ]
]

male["female"] = "0"
female["female"] = "1"

male = male.rename(
    columns={
        "upto_one_and_half_years_male": "upto_one_and_half_years",
        "used_for_breeding_only_male": "used_for_breeding_only",
        "used_for_agriculture_only_male": "used_for_agriculture_only",
        "agriculture_and_breeding_male": "agriculture_and_breeding",
        "bullock_cart_farm_operations_male": "bullock_cart_farm_operations",
        "others_male": "others",
    }
)

cols = male.columns.to_list()

cols = [
    "state_name",
    "district_name",
    "female",
    "breed_type_name",
    "upto_one_and_half_years",
    "used_for_breeding_only",
    "used_for_agriculture_only",
    "agriculture_and_breeding",
    "bullock_cart_farm_operations",
    "others",
    "total_male",
    "total",
]

male = male[cols]

female = female.rename(
    columns={
        "under_one_year_female": "under_one_year",
        "one_to_two_and_half_years_female": "one_to_two_and_half_years",
        "in_milk_female": "in_milk",
        "dry_female": "dry",
        "not_calved_once_female": "not_calved_once",
        "others_female": "others",
    }
)


df = pd.concat([male, female], axis=0)

cols = df.columns.to_list()

cols = [
    "state_name",
    "district_name",
    "female",
    "breed_type_name",
    "upto_one_and_half_years",
    "used_for_breeding_only",
    "used_for_agriculture_only",
    "agriculture_and_breeding",
    "bullock_cart_farm_operations",
    "under_one_ year_female",
    "one_to_two_and_half_years",
    "in_milk",
    "dry",
    "not_calved_once",
    "others",
    "total_male",
    "total_female",
    "total",
]

df = df[cols]

df1 = df.sort_values(by="state_name")

df1.to_csv(Path.joinpath(processed_data_path, "cattle.csv"), index=False)
