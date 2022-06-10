# %%
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px

# reading the data
project_dir = str(Path(__file__).resolve().parents[2])
print(project_dir)
parent_folder = project_dir + "/data/raw/"

gw_master = pd.read_csv(parent_folder + "groundwater_imputed.csv")
# # %%
# # Understanding the profile of this dataset

# profile = pp.ProfileReport(gw_master, minimal=True)

# profile.to_file(parent_folder + "gw_master.html")

# # %%
# loop to know the state profiles

# states = gw_master["state"].unique().tolist()

# for state in states:
#     state_df = gw_master[gw_master["state"] == state]
#     state_profile = pp.ProfileReport(state_df, minimal=True)
#     # all the data profiles for each state is stored in the data/raw/state_profiles folder
#     state_profile.to_file(parent_folder + "state_profiles/" + str(state) + ".html")
# %%

# Cleaning the dataframe
gw_master["year"] = gw_master["year"].astype(str).str.strip()
gw_master["Month"] = gw_master["Month"].astype(str).str.strip()
# %%
# Creating a month year column
gw_master["month_year"] = gw_master["Month"] + ", " + gw_master["year"]
# %%
# Converting that month year column as datetime
gw_master["month_year"] = pd.to_datetime(gw_master["month_year"])

# %%
state_dict = {
    "MP": "Madhya Pradesh",
    "UP": "Uttar Pradesh",
    "MN": "Manipur",
    "ML": "Meghalaya",
    "NL": "Nagaland",
    "TR": "Tripura",
    "AR": "Arunachal Pradesh",
    "CH": "Chandigarh",
    "DL": "Delhi",
    "HR": "Haryana",
    "HP": "Himachal Pradesh",
    "JK": "Jammu and Kashmir",
    "TN": "Tamil Nadu",
    "AP": "Andhra Pradesh",
    "KL": "Kerala",
    "TG": "Telangana",
    "PY": "Pondicherry",
    "KA": "Karnataka",
    "MS": "Maharashtra",
    "GJ": "Gujarat",
    "OD": "Odisha",
    "JH": "Jharkhand",
    "WB": "West Bengal",
    "CG": "Chhattisgarh",
    "AS": "Assam",
    "BR": "Bihar",
    "PB": "Punjab",
    "RJ": "Rajasthan",
    "UK": "Uttarakhand",
    "GA": "Goa",
    "DD": "Daman and Diu",
    "AN": "Andaman and Nicobar Islands",
}
# replacing delhi with DELHI
gw_master = gw_master.replace({"state": state_dict})


# creation of a time dict from report made qualitative research
time_dict = {
    # 'MADHYA PRADESH':'01-01-2020',
    "Maharashtra": "16-04-2012",
    "Rajasthan": "07-04-2006",
    "Delhi": "18-05-2010",
    "Haryana": "26-11-2020",
    "Himachal Pradesh": "27-10-2005",
    "Tamil Nadu": "04-03-2003",
    "Odisha": "29-07-2011",
    "Kerala": "01-08-2002",
    "Karnataka": "05-04-2011",
    "Telangana": "19-04-2002",
    "Andhra Pradesh": "19-04-2002",
    "Chhattisgarh": "01-04-2012",
    "West Bengal": "01-08-2006",
    "Gujarat": "19-09-2001",
    "Assam": "19-05-2012",
    "Bihar": "29-01-2007",
    # 'UTTAR PRADESH':'05-08-2019',
    "Goa": "07-02-2002",
}
# %%

# iterating through each state and generating graphs at state level
for state, pol_date in time_dict.items():
    pol_date = datetime.strptime(pol_date, "%d-%m-%Y")
    state_data = gw_master[gw_master["state"] == state]
    state_data = (
        state_data.groupby(["month_year"]).aggregate("mean").reset_index()
    )
    state_data["status"] = np.where(
        (state_data.month_year > pol_date),
        "After " + str(pol_date),
        "Before " + str(pol_date),
    )
    fig = px.line(
        state_data,
        x="month_year",
        y="water_level",
        color="status",
        title=str(state),
    )
    fig.show()
    file_path = parent_folder + "state_profiles/" + str(state)
    Path(file_path).mkdir(exist_ok=True)
    # fig.savefig()
    fig.write_image(file_path + "/" + str(state) + "_1" + ".jpeg")
    fig.write_html(file_path + "/" + str(state) + "_1" + ".html")
    # alt.Chart(state_data).mark_line().encode(
    #     x = "month_year",
    #     y = "water_level",
    #     color = 'status:N'
    # )
    # fig = sns.relplot(
    #     data=state_data, x="month_year", y="water_level", hue="status", kind="line"
    # )
    # fig.fig.suptitle(str(state))


# %%
# District wise analysis of States
# You need to change the state name

state_name = "Andhra Pradesh"


def district_profile(state_name: str(state_name)):
    state = gw_master[gw_master["state"] == state_name]

    dist_list = state["district"].unique().tolist()

    for each in dist_list:
        district_data = state[state["district"] == each]
        pol_date = datetime.strptime(time_dict[state_name], "%d-%m-%Y")
        district_data = (
            district_data.groupby(["month_year"])
            .aggregate("mean")
            .reset_index()
        )
        district_data["status"] = np.where(
            (district_data.month_year > pol_date),
            "After " + str(pol_date),
            "Before " + str(pol_date),
        )
        fig = px.line(
            district_data,
            x="month_year",
            y="water_level",
            color="status",
            title=str(each),
        )
        fig.show()
        # fig = sns.relplot(
        #     data=district_data, x="month_year", y="water_level", hue="status", kind="line"
        # )
        # fig.fig.suptitle(str(each))
        file_path = (
            parent_folder
            + "state_profiles/"
            + str(state_name)
            + "/"
            + "district_profile"
        )
        Path(file_path).mkdir(exist_ok=True)
        fig.write_image(file_path + "/" + str(each) + "_1" + ".jpeg")
        fig.write_html(file_path + "/" + str(each) + "_1" + ".html")
        # fig.savefig(file_path + "/" + str(each) + "_1" + ".pdf")

    pass


# %%
# Calculating Moving Average for each well.

well_list = gw_master.wlcode.unique().tolist()

# %%
# creating a column for moving average
gw_master["moving_average_well"] = np.nan
# %%
# empty list to append well data with moving average values

well_data_list = []

for well in well_list:
    well_data = gw_master[gw_master["wlcode"] == well].reset_index(
        inplace=False
    )
    well_data.loc[:, "moving_average_well"] = (
        well_data["water_level"].rolling(4, center=True).mean()
    )
    # print(well_data)
    well_data_list.append(well_data)

# %%
# gw_master[gw_master['wlcode'].map(gw_master['wlcode'].value_counts()) == 4].sort_values(by = "wlcode")
