import json
import re
from pathlib import Path

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", "2021_22_March")
interim_path = Path.joinpath(dir_path, "data", "interim", "2021_22_March")
all_names_path = Path.joinpath(interim_path, "all_names.json")

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)


for row in all_names:
    if (
        row["state_name"] == "GUJARAT"
        and row["district_name"] == "DOHAD"
        and row["block_name"] == "FATEPURA"
        and row["gp_name"] == "NANI CHAROLI"
    ):
        row["gp_name"] = re.sub(r"[^A-Za-z0-9_]", "", row["gp_name"])
        print(row["gp_name"])

        ind = all_names.index(row)
        print(all_names.count(row))
        # print(ind)
        # print(all_names[ind+1])

        # row_gp_name=row['gp_name']


print(
    all_names.count(
        {
            "state_name": "GUJARAT",
            "district_name": "DOHAD",
            "block_name": "FATEPURA",
            "gp_name": "NANI CHAROLI",
        }
    )
)
