from pathlib import Path

import pandas as pd
from fuzzywuzzy import process

# Storing directory paths in variables

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")
ext_data_path = Path.joinpath(dir_path, "data", "external")

# Reading and processing LGD Mapper dataframe

lgd = pd.read_csv(Path.joinpath(ext_data_path, "lgd_block.csv"))

lgd.drop(["St_Cs2011_code"], axis=1, inplace=True)
lgd = lgd.rename(
    columns={
        "State Name(In English)": "state",
        "District Name(In English)": "district",
        "Block Name (In English) ": "block",
    }
)

# Creating keys by concatenating state-district-block and state-district

lgd["state_dist_bk"] = ""
for i in range(0, 7216):
    lgd["state_dist_bk"][i] = (
        lgd["state"][i].rstrip() + lgd["district"][i].rstrip() + lgd["block"][i]
    )

lgd["state_dist"] = ""
for i in range(0, 7216):
    lgd["state_dist"][i] = lgd["state"][i].rstrip() + lgd["district"][i].rstrip()

# Read and process PMGSY data

df = pd.read_csv(Path.joinpath(interim_data_path, "merged_data.csv"))

df = df.rename(
    columns={
        "state_name": "state",
        "district_name": "district",
        "block_name": "block",
    }
)

df["state"] = df["state"].str.upper()
df["district"] = df["district"].str.upper()
df["block"] = df["block"].str.upper()

# Manually correcting unmappable values at DISTRICT level

for i in range(0, 425266):
    df["district"][i] = df["district"][i].rstrip(",")
    if df["district"][i].upper() == "MEWAT":
        df["district"][i] = "NUH"
    if df["district"][i].upper() == "ALLAHABAD":
        df["district"][i] = "PRAYAGRAJ"
    if df["district"][i].upper() == "HAJIPUR":
        df["district"][i] = "VAISHALI"
    if df["district"][i].upper() == "BAKHTIYARPUR (PATNA)":
        df["district"][i] = "PATNA"
    if df["district"][i].upper() == "BARH (PATNA)":
        df["district"][i] = "PATNA"
    if df["district"][i].upper() == "EAST CHMAPARAN":
        df["district"][i] = "PURBI CHAMPARAN"
    if df["district"][i].upper() == "EAST CHAMPARAN":
        df["district"][i] = "PURBI CHAMPARAN"
    if df["district"][i].upper() == "MAGARLOAD (DHAMTARI)":
        df["district"][i] = "DHAMTARI"
    if df["district"][i].upper() == "GHUMANHERA":
        df["district"][i] = "SOUTH WEST"
    if df["district"][i].upper() == "NUVEM":
        df["district"][i] = "SOUTH GOA"
    if df["district"][i].upper() == "PERNEM":
        df["district"][i] = "NORTH GOA"
    if df["district"][i].upper() == "KUTCH":
        df["district"][i] = "KACHCHH"
    if df["district"][i].upper() == "DAHOD":
        df["district"][i] = "DOHAD"
    if df["district"][i].upper() == "GURGAON":
        df["district"][i] = "GURUGRAM"
    if df["district"][i].upper() == "JAMMU":
        df["state"][i] = "JAMMU AND KASHMIR"
    if df["district"][i].upper() == "NAGADI":
        df["district"][i] = "RANCHI"
    if df["district"][i].upper() == "GULBARGA, (KALABURAGI)":
        df["district"][i] = "KALABURAGI"
    if df["district"][i].upper() == "GULBARGA":
        df["district"][i] = "KALABURAGI"
    if df["district"][i].upper() == "BIJAPUR (VIJAYPUR)":
        df["district"][i] = "VIJAYAPURA"
    if df["district"][i].upper() == "SIRIGERE (CHITRADURGA)":
        df["district"][i] = "CHITRADURGA"
    if df["district"][i].upper() == "DIGRAS":
        df["district"][i] = "YAVATMAL"
    if df["district"][i].upper() == "BANKI":
        df["district"][i] = "CUTTACK"
    if df["district"][i].upper() == "BADAMBA":
        df["district"][i] = "CUTTACK"
    if df["district"][i].upper() == "BARAMBA":
        df["district"][i] = "CUTTACK"
    if df["district"][i].upper() == "TIGIRIA":
        df["district"][i] = "CUTTACK"
    if df["district"][i].upper() == "BAINA,HINJILICUT":
        df["district"][i] = "GANJAM"
    if df["district"][i].upper() == "HINJILICUT":
        df["district"][i] = "GANJAM"
    if df["district"][i].upper() == "PAKHOWAL":
        df["district"][i] = "LUDHIANA"
    if df["district"][i].upper() == "WEST SIKKIM":
        df["district"][i] = "WEST DISTRICT"
    if df["district"][i].upper() == "EAST SIKKIM":
        df["district"][i] = "EAST DISTRICT"
    if df["district"][i].upper() == "NORTH SIKKIM":
        df["district"][i] = "NORTH DISTRICT"
    if df["district"][i].upper() == "SOUTH SIKKIM":
        df["district"][i] = "SOUTH DISTRICT"
    if df["district"][i].upper() == "SRINAGAR":
        df["state"][i] = "JAMMU AND KASHMIR"
    if df["district"][i].upper() == "THOOTHUKUDI":
        df["district"][i] = "TUTICORIN"
    if df["district"][i].upper() == "BADKOT":
        df["district"][i] = "UTTARKASHI"
    if df["district"][i].upper() == "NOWGAON":
        df["district"][i] = "NAGAON"
    if df["district"][i].upper() == "Y S R KADAPA":
        df["district"][i] = "Y.S.R."
    if df["district"][i].upper() == "KAMRUP RURAL":
        df["district"][i] = "KAMRUP"
    if df["district"][i].upper() == "SIBSAGAR":
        df["district"][i] = "SIVASAGAR"
    if df["district"][i].upper() == "CHAPRA(SARAN)":
        df["district"][i] = "SARAN"
    if df["district"][i].upper() == "WEST CHAMPARAN":
        df["district"][i] = "PASHCHIM CHAMPARAN"
    if df["district"][i].upper() == "MOHINDERGARH":
        df["district"][i] = "MAHENDRAGARH"
    if df["district"][i].upper() == "PASHCHIMI SINGHBHUM":
        df["district"][i] = "WEST SINGHBHUM"
    if df["district"][i].upper() == "PURBI SINGHBHUM":
        df["district"][i] = "EAST SINGHBHUM"
    if df["district"][i].upper() == "BANGALORE R":
        df["district"][i] = "BENGALURU RURAL"
    if df["district"][i].upper() == "BANGALORE U":
        df["district"][i] = "BENGALURU URBAN"
    if df["district"][i].upper() == "BELGAUM":
        df["district"][i] = "BELAGAVI"
    if df["district"][i].upper() == "BELLARY":
        df["district"][i] = "BALLARI"
    if df["district"][i].upper() == "MYSORE":
        df["district"][i] = "MYSURU"
    if df["district"][i].upper() == "BALASORE":
        df["district"][i] = "BALESHWAR"
    if df["district"][i].upper() == "BALASORE":
        df["district"][i] = "BALESHWAR"
    if df["district"][i].upper() == "NORTH 24 PARGANAS":
        df["district"][i] = "24 PARGANAS NORTH"
    if df["district"][i].upper() == "SOUTH 24-PARGANAS":
        df["district"][i] = "24 PARGANAS SOUTH"
    if df["district"][i].upper() == "DAKSHIN DINAJPUR":
        df["district"][i] = "DINAJPUR DAKSHIN"
    if df["district"][i].upper() == "UTTARDINAJPUR":
        df["district"][i] = "DINAJPUR UTTAR"
    if df["district"][i].upper() == "PASCHIM MEDINIPUR":
        df["district"][i] = "MEDINIPUR WEST"
    if df["district"][i].upper() == "PURBA MEDINIPUR":
        df["district"][i] = "MEDINIPUR EAST"
    if df["district"][i].upper() == "N.C.HILLS":
        df["district"][i] = "DIMA HASAO"
    if df["district"][i].upper() == "CHICKBALLAPUR":
        df["district"][i] = "CHIKKABALLAPURA"
    if df["district"][i].upper() == "CHICKMAGALUR":
        df["district"][i] = "CHIKKAMAGALURU"
    if df["district"][i].upper() == "KHANDWA":
        df["district"][i] = "EAST NIMAR"
    if df["district"][i].upper() == "MUKATSAR":
        df["district"][i] = "SRI MUKTSAR SAHIB"
    if df["district"][i].upper() == "NAWASHAHAR":
        df["district"][i] = "SHAHID BHAGAT SINGH NAGAR"
    if df["district"][i].upper() == "EAST":
        df["district"][i] = "EAST DISTRICT"
    if df["district"][i].upper() == "NORTH":
        df["district"][i] = "NORTH DISTRICT"
    if df["district"][i].upper() == "SOUTH":
        df["district"][i] = "SOUTH DISTRICT"
    if df["district"][i].upper() == "FAIZABAD":
        df["district"][i] = "AYODHYA"
    if df["district"][i].upper() == "G.B. NAGAR":
        df["district"][i] = "GAUTAM BUDDHA NAGAR"
    if df["district"][i].upper() == "J.B.F.NAGAR":
        df["district"][i] = "AMROHA"
    if df["district"][i].upper() == "LAKHIMPUR-KHERII":
        df["district"][i] = "KHERI"
    if df["district"][i].upper() == "S.K. NAGAR":
        df["district"][i] = "SANT KABEER NAGAR"
    if df["district"][i].upper() == "SS.R. NAGAR(BHADOHI)":
        df["district"][i] = "BHADOHI"
    if df["district"][i].upper() == "PAURI":
        df["district"][i] = "PAURI GARHWAL"
    if df["district"][i].upper() == "TEHRI":
        df["district"][i] = "TEHRI GARHWAL"
    if df["district"][i].upper() == "SILIGURI M.P":
        df["district"][i] = "DARJEELING"

# Manually correcting unmappable values at BLOCK level

for i in range(0, 425266):
    if df["district"][i].upper() == "JAINTIA":
        if df["block"][i] in ["AMLAREM", "LASKEIN", "THADLASKEIN"]:
            df["district"][i] = "WEST JAINTIA HILLS"
        else:
            df["district"][i] = "EAST JAINTIA HILLS"
    if df["district"][i].upper() == "WARANGAL":
        if df["block"][i].upper() == "NALLABELLY":
            df["block"][i] = "NALLA BELLI"
        elif df["block"][i].upper() == "WARDHANNAPET":
            df["block"][i] = "WARDHANNA PET"
        if df["block"][i] in [
            "NALLA BELLI",
            "ATMAKUR",
            "DUGGONDI",
            "KHANAPUR",
            "NEKKONDA",
            "PARKAL",
            "SANGEM",
            "WARDHANNA PET",
        ]:
            df["district"][i] = "WARANGAL RURAL"
    if df["district"][i].upper() == "Y.S.R.":
        if df["block"][i].upper() == "B MATTAM":
            df["block"][i] = "BRAHMAMGARIMATHAM."
        if df["block"][i].upper() == "C K DINNE":
            df["block"][i] = "CHINTAKOMMA DINNE."
        if df["block"][i].upper() == "CUDDAPAH":
            df["block"][i] = "KADAPA"
        if df["block"][i].upper() == "V N PALLI":
            df["block"][i] = "VEERAPANAYANI PALLE"
    if df["district"][i].upper() == "KHUNTI":
        if df["block"][i].upper() == "TAMAR":
            df["block"][i] = "ARKI"
    if df["district"][i].upper() == "BENGALURU URBAN":
        if df["block"][i].upper() == "BANGALORE(E)":
            df["block"][i] = "BENGALURU EAST"
        if df["block"][i].upper() == "BANGALORE(N)":
            df["block"][i] = "BENGALURU NORTH"
        if df["block"][i].upper() == "BANGALORE(S)":
            df["block"][i] = "BENGALURU SOUTH"
    if df["district"][i].upper() == "AJMER":
        if df["block"][i].upper() == "SILORA (KISHANGARH)":
            df["block"][i] = "Kishangarh SILORA"
    if df["district"][i].upper() == "BARMER":
        if df["block"][i].upper() == "BALOTRA (PACHPADRA)":
            df["block"][i] = "BALTORA"

# Creating keys by concatenating state-dist and state-dist-block to be used while merging the dataframes

df["state_dist"] = df["state"] + df["district"]
df["state_dist_bk"] = df["state"] + df["district"] + df["block"]

# Merging PMGSY and LGD dataframes on state-dist-block key

df1 = pd.merge(
    df,
    lgd,
    how="outer",
    left_on="state_dist_bk",
    right_on="state_dist_bk",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

# Collecting unmapped values

not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
    [
        "state_DATA",
        "district_DATA",
        "block_DATA",
        "state_dist_DATA",
        "state_dist_bk",
    ]
]

not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_bk")

# Initiating Fuzzy Matching

result1 = [
    process.extractOne(i, lgd["state_dist_bk"])
    for i in not_lgd_mapped["state_dist_bk"][:800]
]

result2 = [
    process.extractOne(i, lgd["state_dist_bk"])
    for i in not_lgd_mapped["state_dist_bk"][800:1600]
]

result3 = [
    process.extractOne(i, lgd["state_dist_bk"])
    for i in not_lgd_mapped["state_dist_bk"][1600:2400]
]

result4 = [
    process.extractOne(i, lgd["state_dist_bk"])
    for i in not_lgd_mapped["state_dist_bk"][2400:2626]
]

result = []

for i in range(0, len(result1)):
    result.append(result1[i])
for i in range(0, len(result2)):
    result.append(result2[i])
for i in range(0, len(result3)):
    result.append(result3[i])
for i in range(0, len(result4)):
    result.append(result4[i])

result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist_bk"], index=None)
    .reset_index()
    .drop("index", axis=1)
)

mapper_df = pd.concat(
    [not_lgd_proxy_df, result],
    axis=1,
    ignore_index=True,
    names=["original", "match", "score"],
)


mapper_df = mapper_df[mapper_df[2] >= 88]
mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))
df["state_dist_bk"] = df["state_dist_bk"].replace(mapper_dict)

# Merging once again after fuzzy matching

df1 = pd.merge(
    df,
    lgd,
    how="outer",
    left_on="state_dist_bk",
    right_on="state_dist_bk",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
    [
        "state_DATA",
        "district_DATA",
        "block_DATA",
        "state_dist_DATA",
        "state_dist_bk",
    ]
]


not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_bk")

# Initiating fuzzy matching again

result = [
    process.extractOne(i, lgd["state_dist_bk"]) for i in not_lgd_mapped["state_dist_bk"]
]

result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)

not_lgd_proxy_df = (
    pd.DataFrame(not_lgd_mapped["state_dist_bk"], index=None)
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
df["state_dist_bk"] = df["state_dist_bk"].replace(mapper_dict)

# Merging dataframes again

df1 = pd.merge(
    df,
    lgd,
    how="outer",
    left_on="state_dist_bk",
    right_on="state_dist_bk",
    validate="m:1",
    indicator=True,
    suffixes=["_DATA", "_LGD"],
)

not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
    [
        "state_DATA",
        "district_DATA",
        "block_DATA",
        "state_dist_DATA",
        "state_dist_bk",
    ]
]

not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist_bk")

# Storing unmapped PMGSY data

not_lgd_mapped.to_csv("pmgsy_unmapped.csv")

# Cleaning the merged and mapped dataset

df2 = df1[df1["_merge"] == "both"]

df2.drop(["_merge"], axis=1, inplace=True)
df2.drop(
    ["state_dist_LGD", "block_LGD", "state_LGD", "district_LGD"],
    axis=1,
    inplace=True,
)


cols = df2.columns.to_list()

cols = [
    "sr.no._x",
    "state_DATA",
    "district_DATA",
    "block_DATA",
    "state_dist_DATA",
    "state_dist_bk",
    "St_LGD_code",
    "Dt_LGD_code",
    "Bk_LGD_code",
    "packages",
    "sanctioned year_x",
    "road name",
    "upgrade / new",
    "surface type",
    "length",
    "pavement cost",
    "no. of cd works",
    "cd work cost",
    "lsb cost",
    "lsb state cost",
    "protection work",
    "other works",
    "present status",
    "completed length",
    "expenditure till date",
    "total cost",
    "view",
    "habitation name",
    "population",
    "sc/st population",
    "sr.no._y",
    "state name",
    "district name",
    "block name",
    "package id",
    "sanctioned year_y",
    "road length",
    "work award date",
    "completion date",
    "status",
    "contractor name",
    "company name",
    "total population",
]

df2 = df2[cols]

df2 = df2.rename(
    columns={
        "state_DATA": "state",
        "district_DATA": "district",
        "block_DATA": "block",
        "state_dist_DATA": "state_dist",
    }
)

df2.to_csv(Path.joinpath(processed_data_path, "pmgsy_lgd.csv"), index=False)
