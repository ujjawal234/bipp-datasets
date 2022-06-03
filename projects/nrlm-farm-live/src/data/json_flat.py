import json
from pathlib import Path

"""This scripts flattens the the lists of nested disctionaries and creating a new flat list with  year and month value dictionaries"""


# FLATTENING INTO A SINGLE LIST OF DICTIONARIES
# defining a list for storing all the names of states, district, blocks and GP's
all_names = []
# year_1 = []
# year_2 = []

# calling all the json files
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", "jsons")
interim_path = Path.joinpath(dir_path, "data", "interim", "jsons")


all_names_path = Path.joinpath(interim_path, "all_names_extended_new.json")
year_1_path = Path.joinpath(interim_path, "year1.json")
year_2_path = Path.joinpath(interim_path, "year2.json")
year_3_path = Path.joinpath(interim_path, "year3.json")
# year_month_path = Path.joinpath(interim_path, "year_month.json")

if not interim_path.exists():
    interim_path.mkdir(parents=True)


files = list(raw_path.glob("*/*/*.json"))

# concatenating all the jsons
for file in files:

    with open(file, "r", errors="ignore", encoding="utf-8") as outfile:
        print(file)
        dist_level_names = json.load(outfile)

    all_names.extend(dist_level_names)

year_1 = all_names
year_2 = all_names
year_3 = all_names


with open(str(year_1_path), "w") as outfile:
    json.dump(year_1, outfile, ensure_ascii=False)

with open(str(year_2_path), "w") as outfile:
    json.dump(year_2, outfile, ensure_ascii=False)

with open(str(year_3_path), "w") as outfile:
    json.dump(year_3, outfile, ensure_ascii=False)


# cleaning year 1
with open(year_1_path, "r", errors="ignore", encoding="utf-8") as outfile:
    year1 = json.load(outfile)

# replacing year values as 2022-23 for all_names
for row in year1:
    row["year"] = "2022-2023"


# cleaning year 2
with open(year_3_path, "r", errors="ignore", encoding="utf-8") as outfile:
    year3 = json.load(outfile)

# replacing year values as 2020-2021 for all_names
for i in year3:
    i["year"] = "2020-2021"


# cleaning year 2
with open(year_2_path, "r", errors="ignore", encoding="utf-8") as outfile:
    year2 = json.load(outfile)
# need not clean year_2 becuase default year value is 2021-2022


# appending all cleaned lists into master list and exporting it
all_names = []
# all_names.extend(year1) #activate this when 2022-2023 months are active
all_names.extend(year2)
all_names.extend(year3)

# Adding month values and month names to the master file
month_values = list(range(1, 13))
print(month_values)

month_names = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
month_names = [x.upper() for x in month_names]
print(month_names)

all_names_ext = []

for month_name, month_value in zip(month_names, month_values):
    for k in all_names:
        k["month"] = month_name
        k["month_code"] = str(month_value)

    month_path = Path.joinpath(interim_path, ".".join([month_name, "json"]))

    with open(str(month_path), "w") as outfile:
        json.dump(all_names, outfile, ensure_ascii=False)

    with open(str(month_path), "r", errors="ignore", encoding="utf-8") as outfile:
        month_list = json.load(outfile)

    # print(len(month_list))
    # print(month_list[0])
    # print(month_list[len(month_list)-1])

    all_names_ext.extend(month_list)

# SPECIAL CONDITION BASED FILTERING. REMOVE WHEN THE 2022-2023 webpage is updated
# removing all months except April for year==2022-2023 becuase this year (as of 30th May 2022) has only APRIL in month dropdown lists
for k in year1:
    k["month"] = "APRIL"
    k["month_code"] = "4"

all_names_ext.extend(year1)
########################################################################


print(len(all_names))
print(len(all_names_ext))


print(all_names_ext[0])
print(all_names_ext[len(all_names_ext) - 1])

# ****************************************************************************

with open(str(all_names_path), "w") as outfile:
    json.dump(all_names_ext, outfile, ensure_ascii=False)

print("The master list of names has been exported as json into the Interim directory")
