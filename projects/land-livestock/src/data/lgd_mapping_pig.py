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

pig = pd.read_csv(Path.joinpath(processed_data_path, "pig.csv"))

for i in range(0, 1252):
    if pig["district_name"][i].upper() == "SORAIDEU":
        pig["district_name"][i] = "CHARAIDEO"
    if pig["district_name"][i].upper() == "SIBSAGAR":
        pig["district_name"][i] = "SIVASAGAR"
    if pig["district_name"][i].upper() == "MEWAT":
        pig["district_name"][i] = "NUH"
    if pig["district_name"][i].upper() == "KARGIL":
        pig["state_name"][i] = "LADAKH"
    if pig["district_name"][i].upper() == "LEH LADAKH":
        pig["state_name"][i] = "LADAKH"
    if pig["district_name"][i].upper() == "DADRA AND NAGAR HAVELI":
        pig["state_name"][i] = "THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU"
    if pig["district_name"][i].upper() == "GULBARGA":
        pig["district_name"][i] = "KALABURAGI"
    if pig["district_name"][i].upper() == "MYSORE":
        pig["district_name"][i] = "MYSURU"
    if pig["district_name"][i].upper() == "BANGALORE RURAL":
        pig["district_name"][i] = "BENGALURU RURAL"
    if pig["district_name"][i].upper() == "SORAIDEU":
        pig["district_name"][i] = "CHARAIDEO"
    if pig["district_name"][i].upper() == "BELGAUM":
        pig["district_name"][i] = "BELAGAVI"
    if pig["district_name"][i].upper() == "BELLARY":
        pig["district_name"][i] = "BALLARI"
    if pig["district_name"][i].upper() in "NAWANSHAHR (SBS NAGAR)":
        pig["district_name"][i] = "SHAHID BHAGAT SINGH NAGAR"
    if pig["district_name"][i].upper() == "MUKTSAR":
        pig["district_name"][i] = "SRI MUKTSAR SAHIB"
    if pig["district_name"][i].upper() == "ALLAHABAD":
        pig["district_name"][i] = "PRAYAGRAJ"
    if pig["district_name"][i].upper() == "SANT RAVIDAS NAGAR":
        pig["district_name"][i] = "BHADOHI"
    if pig["district_name"][i].upper() == "BARDHAMAN":
        pig["district_name"][i] = "PURBA BARDHAMAN"
    if (pig["district_name"][i].upper() == "BIJAPUR") and (
        pig["state_name"][i].upper() == "KARNATAKA"
    ):
        pig["district_name"][i] = "VIJAYAPURA"


pig["state_name"] = pig["state_name"].str.upper()
pig["district_name"] = pig["district_name"].str.upper()
pig["state_dist"] = pig["state_name"] + pig["district_name"]

g1 = pd.merge(
    pig,
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
pig["state_dist"] = pig["state_dist"].replace(mapper_dict)

g1 = pd.merge(
    pig,
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
pig["state_dist"] = pig["state_dist"].replace(mapper_dict)

g1 = pd.merge(
    pig,
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

not_lgd_mapped.to_csv("pig_unmapped.csv")

df = g1[g1["_merge"] == "both"]
df = df.drop(["_merge", "state", "district"], axis=1)

cols = [
    "state_name",
    "district_name",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
    "breed_type_name",
    "under_six_months",
    "six_months_and_above",
    "total",
]

df = df[cols]

df.to_csv(Path.joinpath(processed_data_path, "pig_lgd.csv"), index=False)

pig = pd.read_csv(Path.joinpath(processed_data_path, "pig_lgd.csv"))

pig.drop(["state_dist"], axis=1, inplace=True)
pig.rename(
    columns={
        "St_LGD_code": "state_lgd_code",
        "Dt_LGD_code": "district_lgd_code",
    },
    inplace=True,
)

pig.to_csv(Path.joinpath(processed_data_path, "pig_lgd.csv"), index=False)
