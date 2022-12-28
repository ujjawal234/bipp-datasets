from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")
ext_data_path = Path.joinpath(dir_path, "data", "external")

lgd = pd.read_csv(Path.joinpath(ext_data_path, "lgd_district.csv"))


state = {}
district = {}

for i in range(0, len(lgd["State Name(In English)"])):
    state[lgd["State Name(In English)"][i].rstrip()] = lgd["St_LGD_code"][i]
for j in range(0, len(lgd["District Name(In English)"])):
    district[
        lgd["State Name(In English)"][j].rstrip() + lgd["District Name(In English)"][j]
    ] = lgd["Dt_LGD_code"][j]

ad_fem = pd.read_csv(Path.joinpath(processed_data_path, "adult_female_population.csv"))
nddb = pd.read_csv(Path.joinpath(processed_data_path, "NDDB_Statewise.csv"))
pcmc = pd.read_csv(
    Path.joinpath(processed_data_path, "Per Capita Monthly Consumption.csv")
)


var_names = [x.lower() for x in nddb.columns]
nddb.columns = var_names

ad = {}
n = {}
p = {}

ad_fem["state_lgd"] = ""
nddb["state_lgd"] = ""

for i in range(0, 37):
    st = ad_fem["state"][i].upper()
    if st in state:
        ad_fem["state_lgd"][i] = state[st]
    else:
        ad_fem["state_lgd"][i] = "0"
        ad[st] = "0"

for i in range(0, 107):
    st = nddb["state"][i].upper()
    if st in state:
        nddb["state_lgd"][i] = state[st]
    else:
        nddb["state_lgd"][i] = "0"
        n[st] = "0"

ad["A & N ISLANDS"] = 35
ad["D & N HAVELI"] = 38
ad["DAMAN & DIU"] = 38
ad["JAMMU & KASHMIR"] = 1

for i in range(0, 37):
    if ad_fem["state_lgd"][i] == "0":
        st = ad_fem["state"][i].upper()
        ad_fem["state_lgd"][i] = ad[st]


n["JAMMU & KASHMIR"] = 1
n["A&N ISLANDS"] = 35
n["D & N HAVELI"] = 38
n["DAMAN & DIU"] = 38
n["TAMILNADU"] = 33
n["PONDICHERRY"] = 34

for i in range(0, 107):
    if nddb["state_lgd"][i] == "0":
        st = nddb["state"][i].upper()
        nddb["state_lgd"][i] = n[st]


print("Number of missing values (state):", end="\n")
print("Adult Female Population", len(ad_fem[ad_fem["state_lgd"] == "0"]))
print("NDDB", len(nddb[nddb["state_lgd"] == "0"]))

ad_fem.drop(["Unnamed: 0", "Unnamed: 0.1"], axis=1, inplace=True)

nddb.to_csv(Path.joinpath(processed_data_path, "NDDB_Statewise.csv"), index=False)
ad_fem.to_csv(
    Path.joinpath(processed_data_path, "adult_female_population.csv"), index=False
)

ad_fem = ad_fem.drop(["Unnamed: 0"], axis=1)
nddb.drop(["unnamed: 0"], axis=1, inplace=True)

col_list = [
    "state",
    "state_lgd",
    "crossbred_cows_2.5yrs",
    "indigenous_cows_3yrs",
    "total_cows",
    "female_buffaloes_3yrs",
    "total_cows_buffaloes",
]

ad_fem = ad_fem[col_list]
nddb.columns.to_list()

col_list = [
    "state",
    "state_lgd",
    "2001-02",
    "2002-03",
    "2003-04",
    "2004-05",
    "2005-06",
    "2006-07",
    "2007-08",
    "2008-09",
    "2009-10",
    "2010-11",
    "2011-12",
    "2012-13",
    "2013-14",
    "2014-15",
    "2015-16",
    "2016-17",
    "2017-18",
    "2018-19",
    "type",
]

nddb = nddb[col_list]


nddb.to_csv(Path.joinpath(processed_data_path, "NDDB_Statewise_lgd.csv"), index=False)
ad_fem.to_csv(
    Path.joinpath(processed_data_path, "adult_female_population_lgd.csv"), index=False
)

data = pd.read_csv(Path.joinpath(processed_data_path, "NDDB_Statewise_lgd.csv"))

data.drop(["Unnamed: 0"], axis=1, inplace=True)
col_list = data.columns.to_list()


col_list = [
    "state",
    "state_lgd",
    "type",
    "2001-02",
    "2002-03",
    "2003-04",
    "2004-05",
    "2005-06",
    "2006-07",
    "2007-08",
    "2008-09",
    "2009-10",
    "2010-11",
    "2011-12",
    "2012-13",
    "2013-14",
    "2014-15",
    "2015-16",
    "2016-17",
    "2017-18",
    "2018-19",
]


data = data[col_list]

data.to_csv(Path.joinpath(processed_data_path, "NDDB_Statewise_lgd.csv"), index=False)

ad_fem.columns.to_list


col_list = [
    "state",
    "state_lgd",
    "crossbred_cows_2.5yrs",
    "indigenous_cows_3yrs",
    "total_cows",
    "female_buffaloes_3yrs",
    "total_cows_buffaloes",
]
ad_fem = ad_fem[col_list]


ad_fem.to_csv(
    Path.joinpath(processed_data_path, "adult_female_population_lgd.csv"), index=False
)
