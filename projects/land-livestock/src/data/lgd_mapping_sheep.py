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

sheep = pd.read_csv(Path.joinpath(processed_data_path, "sheep.csv"))

for i in range(0, 1260):
    if sheep["district_name"][i].upper() == "SORAIDEU":
        sheep["district_name"][i] = "CHARAIDEO"
    if sheep["district_name"][i].upper() == "SIBSAGAR":
        sheep["district_name"][i] = "SIVASAGAR"
    if sheep["district_name"][i].upper() == "MEWAT":
        sheep["district_name"][i] = "NUH"
    if sheep["district_name"][i].upper() == "KARGIL":
        sheep["state_name"][i] = "LADAKH"
    if sheep["district_name"][i].upper() == "LEH LADAKH":
        sheep["state_name"][i] = "LADAKH"
    if sheep["district_name"][i].upper() == "DADRA AND NAGAR HAVELI":
        sheep["state_name"][i] = "THE DADRA AND NAGAR HAVELI AND DAMAN AND DIU"
    if sheep["district_name"][i].upper() == "GULBARGA":
        sheep["district_name"][i] = "KALABURAGI"
    if sheep["district_name"][i].upper() == "MYSORE":
        sheep["district_name"][i] = "MYSURU"
    if sheep["district_name"][i].upper() == "BANGALORE RURAL":
        sheep["district_name"][i] = "BENGALURU RURAL"
    if sheep["district_name"][i].upper() == "SORAIDEU":
        sheep["district_name"][i] = "CHARAIDEO"
    if sheep["district_name"][i].upper() == "BELGAUM":
        sheep["district_name"][i] = "BELAGAVI"
    if sheep["district_name"][i].upper() == "BELLARY":
        sheep["district_name"][i] = "BALLARI"
    if sheep["district_name"][i].upper() in "NAWANSHAHR (SBS NAGAR)":
        sheep["district_name"][i] = "SHAHID BHAGAT SINGH NAGAR"
    if sheep["district_name"][i].upper() == "MUKTSAR":
        sheep["district_name"][i] = "SRI MUKTSAR SAHIB"
    if sheep["district_name"][i].upper() == "ALLAHABAD":
        sheep["district_name"][i] = "PRAYAGRAJ"
    if sheep["district_name"][i].upper() == "SANT RAVIDAS NAGAR":
        sheep["district_name"][i] = "BHADOHI"
    if sheep["district_name"][i].upper() == "BARDHAMAN":
        sheep["district_name"][i] = "PURBA BARDHAMAN"
    if (sheep["district_name"][i].upper() == "BIJAPUR") and (
        sheep["state_name"][i].upper() == "KARNATAKA"
    ):
        sheep["district_name"][i] = "VIJAYAPURA"


sheep["state_name"] = sheep["state_name"].str.upper()
sheep["district_name"] = sheep["district_name"].str.upper()
sheep["state_dist"] = sheep["state_name"] + sheep["district_name"]

b1 = pd.merge(
    sheep,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = b1[(b1["_merge"] == "left_only")][
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
sheep["state_dist"] = sheep["state_dist"].replace(mapper_dict)

b1 = pd.merge(
    sheep,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = b1[(b1["_merge"] == "left_only")][
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
sheep["state_dist"] = sheep["state_dist"].replace(mapper_dict)

b1 = pd.merge(
    sheep,
    lgd,
    how="outer",
    left_on="state_dist",
    right_on="state_dist",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = b1[(b1["_merge"] == "left_only")][
    [
        "state_name",
        "district_name",
        "state_dist",
    ]
]
not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

not_lgd_mapped.to_csv("sheep_unmapped.csv")

df = b1[b1["_merge"] == "both"]

df = df.drop(["_merge", "state", "district"], axis=1)

cols = [
    "state_name",
    "district_name",
    "state_dist",
    "St_LGD_code",
    "Dt_LGD_code",
    "breed_type_name",
    "upto_one_year",
    "one_year_and_above",
    "total",
]

df = df[cols]

df.to_csv(Path.joinpath(processed_data_path, "sheep_lgd.csv"), index=False)

sheep = pd.read_csv(Path.joinpath(processed_data_path, "sheep_lgd.csv"))

sheep.drop(["state_dist"], axis=1, inplace=True)
sheep.rename(
    columns={
        "St_LGD_code": "state_lgd_code",
        "Dt_LGD_code": "district_lgd_code",
    },
    inplace=True,
)

sheep.to_csv(Path.joinpath(processed_data_path, "sheep_lgd.csv"), index=False)
