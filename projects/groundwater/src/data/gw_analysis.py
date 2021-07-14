# %%
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_profiling as pp
import seaborn as sns

# reading the data
project_dir = str(Path(__file__).resolve().parents[2])
print(project_dir)
parent_folder = project_dir + "/data/raw/"

gw_master = pd.read_csv(parent_folder + "Ground_water_level.csv")
# # %%
# # Understanding the profile of this dataset

# profile = pp.ProfileReport(gw_master, minimal=True)

# profile.to_file(parent_folder + "gw_master.html")

# # %%
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

# replacing delhi with DELHI
gw_master["State_name"] = gw_master["State_name"].replace("delhi", "DELHI")


# creation of a time dict from report made qualitative research
time_dict = {
    # 'MADHYA PRADESH':'01-01-2020',
    "MAHARASHTRA": "16-04-2012",
    "RAJASTHAN": "07-04-2006",
    "DELHI": "18-05-2010",
    "HARYANA": "26-11-2020",
    "HIMACHAL PRADESH": "27-10-2005",
    "TAMIL NADU": "04-03-2003",
    "ODISHA": "29-07-2011",
    "KERALA": "01-08-2002",
    "KARNATAKA": "05-04-2011",
    "TELANGANA": "19-04-2002",
    "ANDHRA PRADESH": "19-04-2002",
    "CHHATTISGARH": "01-04-2012",
    "WEST BENGAL": "01-08-2006",
    "GUJARAT": "19-09-2001",
    "ASSAM": "19-05-2012",
    "BIHAR": "29-01-2007",
    # 'UTTAR PRADESH':'05-08-2019',
    "GOA": "07-02-2002",
}

# iterating through each state and generating graphs at state level
for state, pol_date in time_dict.items():
    pol_date = datetime.strptime(pol_date, "%d-%m-%Y")
    state_data = gw_master[gw_master["State_name"] == state]
    state_data = (
        state_data.groupby(["month_year", "State_name"]).aggregate("mean").reset_index()
    )
    state_data["status"] = np.where(
        (state_data.month_year > pol_date),
        "After " + str(pol_date),
        "Before " + str(pol_date),
    )
    fig = sns.relplot(
        data=state_data, x="month_year", y="gwl", hue="status", kind="line"
    )
    fig.fig.suptitle(str(state))
    file_path = parent_folder + "state_profiles/" + str(state)
    Path(file_path).mkdir(exist_ok=True)
    fig.savefig(file_path + "/" + str(state) + "_1" + ".pdf")

# %%
