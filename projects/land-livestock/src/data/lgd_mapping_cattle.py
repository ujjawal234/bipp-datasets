from pathlib import Path

import pandas as pd
from fuzzywuzzy import process

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")
ext_data_path = Path.joinpath(dir_path, "data", "external")

lgd = pd.read_csv(Path.joinpath(ext_data_path, "lgd_district.csv"))

lgd.drop(
    ["St_Cs2011_code", "St_Cs2001_code", "Dt_Cs2011_code", "Dt_Cs2001_code"],
    axis=1,
    inplace=True,
)

lgd = lgd.rename(
    columns={
        "State Name(In English)": "state",
        "District Name(In English)": "district",
    }
)

lgd["state_dist"] = ""
for i in range(0, 734):
    lgd["state_dist"][i] = lgd["state"][i].rstrip() + lgd["district"][i]

cattle = pd.read_csv(Path.joinpath(processed_data_path, "cattle.csv"))

cattle["state_name"] = cattle["state_name"].str.upper()
cattle["district_name"] = cattle["district_name"].str.upper()
cattle["state_dist"] = cattle["state_name"] + cattle["district_name"]

c1 = pd.merge(
    cattle,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = c1[(c1["_merge"] == "left_only")][
    [
        "state_name",
        "district_name",
        "state_dist",
    ]
]

not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

result = [
    process.extractOne(i, lgd["state_dist"]) for i in not_lgd_mapped["state_dist"]
]
result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist"], index=None)
    .reset_index()
    .drop("index", axis=1)
)

mapper_df = pd.concat(
    [not_lgd_proxy_df, result],
    axis=1,
    ignore_index=True,
    names=["original", "match", "score"],
)

mapper_df = mapper_df[mapper_df[2] >= 90]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
cattle["state_dist"] = cattle["state_dist"].replace(mapper_dict)

c1 = pd.merge(
    cattle,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = c1[(c1["_merge"] == "left_only")][
    [
        "state_name",
        "district_name",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

result = [
    process.extractOne(i, lgd["state_dist"]) for i in not_lgd_mapped["state_dist"]
]
result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist"], index=None)
    .reset_index()
    .drop("index", axis=1)
)

mapper_df = pd.concat(
    [not_lgd_proxy_df, result],
    axis=1,
    ignore_index=True,
    names=["original", "match", "score"],
)

mapper_df = mapper_df[mapper_df[2] >= 90]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
cattle["state_dist"] = cattle["state_dist"].replace(mapper_dict)

c1 = pd.merge(
    cattle,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = c1[(c1["_merge"] == "left_only")][
    [
        "state_name",
        "district_name",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

not_lgd_mapped.to_csv("cattle_unmapped.csv")

for i in range(0, 2776):
    if cattle["district_name"][i].upper() == "SORAIDEU":
        cattle["district_name"][i] = "CHARAIDEO"
    if cattle["district_name"][i].upper() == "SIBSAGAR":
        cattle["district_name"][i] = "SIVASAGAR"
    if cattle["district_name"][i].upper() == "MEWAT":
        cattle["district_name"][i] = "NUH"
    if cattle["district_name"][i].upper() == "KARGIL":
        cattle["state_name"][i] = "LADAKH"
    if cattle["district_name"][i].upper() == "LEH LADAKH":
        cattle["state_name"][i] = "LADAKH"
    if cattle["district_name"][i].upper() == "DADRA AND NAGAR HAVELI":
        cattle["state_name"][i] = "THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU"
    if cattle["district_name"][i].upper() == "GULBARGA":
        cattle["district_name"][i] = "KALABURAGI"
    if cattle["district_name"][i].upper() == "MYSORE":
        cattle["district_name"][i] = "MYSURU"
    if cattle["district_name"][i].upper() == "BANGALORE RURAL":
        cattle["district_name"][i] = "BENGALURU RURAL"
    if cattle["district_name"][i].upper() == "SORAIDEU":
        cattle["district_name"][i] = "CHARAIDEO"
    if cattle["district_name"][i].upper() == "BELGAUM":
        cattle["district_name"][i] = "BELAGAVI"
    if cattle["district_name"][i].upper() == "BELLARY":
        cattle["district_name"][i] = "BALLARI"
    if cattle["district_name"][i].upper() in "NAWANSHAHR (SBS NAGAR)":
        cattle["district_name"][i] = "SHAHID BHAGAT SINGH NAGAR"
    if cattle["district_name"][i].upper() == "MUKTSAR":
        cattle["district_name"][i] = "SRI MUKTSAR SAHIB"
    if cattle["district_name"][i].upper() == "ALLAHABAD":
        cattle["district_name"][i] = "PRAYAGRAJ"
    if cattle["district_name"][i].upper() == "SANT RAVIDAS NAGAR":
        cattle["district_name"][i] = "BHADOHI"
    if cattle["district_name"][i].upper() == "BARDHAMAN":
        cattle["district_name"][i] = "PURBA BARDHAMAN"
    if (cattle["district_name"][i].upper() == "BIJAPUR") and (
        cattle["state_name"][i].upper() == "KARNATAKA"
    ):
        cattle["district_name"][i] = "VIJAYAPURA"

cattle.to_csv(Path.joinpath(processed_data_path, "cattle.csv"), index=False)

c2 = c1[c1["_merge"] == "both"]
c2 = c2.drop(["_merge", "state", "district"], axis=1)


cols = [
    "state_name",
    "district_name",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
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

c2 = c2[cols]
c2.to_csv(Path.joinpath(processed_data_path, "cattle_lgd.csv"), index=False)

cattle = pd.read_csv(Path.joinpath(processed_data_path, "cattle_lgd.csv"))

cattle.drop(["state_dist"], axis=1, inplace=True)
cattle.rename(
    columns={
        "St_LGD_code": "state_lgd_code",
        "Dt_LGD_code": "district_lgd_code",
    },
    inplace=True,
)

cattle.to_csv(Path.joinpath(processed_data_path, "cattle_lgd.csv"), index=False)
