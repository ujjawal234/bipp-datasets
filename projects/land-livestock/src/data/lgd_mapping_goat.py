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

goat = pd.read_csv(Path.joinpath(processed_data_path, "goat.csv"))

for i in range(0, 1396):
    if goat["district_name"][i].upper() == "SORAIDEU":
        goat["district_name"][i] = "CHARAIDEO"
    if goat["district_name"][i].upper() == "SIBSAGAR":
        goat["district_name"][i] = "SIVASAGAR"
    if goat["district_name"][i].upper() == "MEWAT":
        goat["district_name"][i] = "NUH"
    if goat["district_name"][i].upper() == "KARGIL":
        goat["state_name"][i] = "LADAKH"
    if goat["district_name"][i].upper() == "LEH LADAKH":
        goat["state_name"][i] = "LADAKH"
    if goat["district_name"][i].upper() == "DADRA AND NAGAR HAVELI":
        goat["state_name"][i] = "THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU"
    if goat["district_name"][i].upper() == "GULBARGA":
        goat["district_name"][i] = "KALABURAGI"
    if goat["district_name"][i].upper() == "MYSORE":
        goat["district_name"][i] = "MYSURU"
    if goat["district_name"][i].upper() == "BANGALORE RURAL":
        goat["district_name"][i] = "BENGALURU RURAL"
    if goat["district_name"][i].upper() == "SORAIDEU":
        goat["district_name"][i] = "CHARAIDEO"
    if goat["district_name"][i].upper() == "BELGAUM":
        goat["district_name"][i] = "BELAGAVI"
    if goat["district_name"][i].upper() == "BELLARY":
        goat["district_name"][i] = "BALLARI"
    if goat["district_name"][i].upper() in "NAWANSHAHR (SBS NAGAR)":
        goat["district_name"][i] = "SHAHID BHAGAT SINGH NAGAR"
    if goat["district_name"][i].upper() == "MUKTSAR":
        goat["district_name"][i] = "SRI MUKTSAR SAHIB"
    if goat["district_name"][i].upper() == "ALLAHABAD":
        goat["district_name"][i] = "PRAYAGRAJ"
    if goat["district_name"][i].upper() == "SANT RAVIDAS NAGAR":
        goat["district_name"][i] = "BHADOHI"
    if goat["district_name"][i].upper() == "BARDHAMAN":
        goat["district_name"][i] = "PURBA BARDHAMAN"
    if (goat["district_name"][i].upper() == "BIJAPUR") and (
        goat["state_name"][i].upper() == "KARNATAKA"
    ):
        goat["district_name"][i] = "VIJAYAPURA"

goat["state_name"] = goat["state_name"].str.upper()
goat["district_name"] = goat["district_name"].str.upper()
goat["state_dist"] = goat["state_name"] + goat["district_name"]

g1 = pd.merge(
    goat,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = g1[(g1["_merge"] == "left_only")][
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
goat["state_dist"] = goat["state_dist"].replace(mapper_dict)

g1 = pd.merge(
    goat,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = g1[(g1["_merge"] == "left_only")][
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
goat["state_dist"] = goat["state_dist"].replace(mapper_dict)

g1 = pd.merge(
    goat,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = g1[(g1["_merge"] == "left_only")][
    [
        "state_name",
        "district_name",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

not_lgd_mapped.to_csv("goat_unmapped.csv")

df = g1[g1["_merge"] == "both"]

df = df.drop(["_merge", "state", "district"], axis=1)

cols = [
    "state_name",
    "district_name",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
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

df.to_csv(Path.joinpath(processed_data_path, "goat_lgd.csv"), index=False)

goat = pd.read_csv(Path.joinpath(processed_data_path, "goat_lgd.csv"))

goat.drop(["state_dist"], axis=1, inplace=True)
goat.rename(
    columns={
        "St_LGD_code": "state_lgd_code",
        "Dt_LGD_code": "district_lgd_code",
    },
    inplace=True,
)

goat.to_csv(Path.joinpath(processed_data_path, "goat_lgd.csv"), index=False)
