# %%
from pathlib import Path

import pandas as pd
import pandas_profiling as pp

# reading the data
project_dir = str(Path(__file__).resolve().parents[2])
print(project_dir)
parent_folder = project_dir + "/data/raw/"

gw_master = pd.read_csv(parent_folder + "Ground_water_level.csv")
# %%
# Understanding the profile of this dataset

profile = pp.ProfileReport(gw_master, minimal=True)

profile.to_file(parent_folder + "gw_master.html")

# %%
# loop to know the state profiles

states = gw_master["State_name"].unique().tolist()

for state in states:
    state_df = gw_master[gw_master["State_name"] == state]
    state_profile = pp.ProfileReport(state_df, minimal=True)
    # all the data profiles for each state is stored in the data/raw/state_profiles folder
    state_profile.to_file(parent_folder + "state_profiles/" + str(state) + ".html")
# %%

# Cleaning the dataframe
gw_master["year"] = gw_master["year"].astype(str).str.strip()
gw_master["month"] = gw_master["month"].astype(str).str.strip()
# %%
# Creating a month year column
gw_master["month_year"] = gw_master["month"] + ", " + gw_master["year"]
# %%
# Converting that month year column as datetime
gw_master["month_year"] = pd.to_datetime(gw_master["month_year"])

# %%
