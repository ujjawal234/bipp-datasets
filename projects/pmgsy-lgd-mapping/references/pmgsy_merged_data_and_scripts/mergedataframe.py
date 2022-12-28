import os
from pathlib import Path

import pandas as pd

# from pandas.io.parsers import read_csv

# Getting path directory
project_folder = str(Path(__file__).resolve().parents[2])
print(project_folder)
phy_pro_path = project_folder + r"/data/processed/physical_progress_dataset.csv"
phy_fin_mon_path = project_folder + r"/data/processed/physical_financial_monitoring.csv"
phy_pro = pd.read_csv(phy_pro_path)
print(phy_pro)

phy_fin_mon = pd.read_csv(phy_fin_mon_path)
state_list = phy_pro["state_name"].unique().tolist()

dframe = []
phy_pro, phy_fin_mon,
for state in state_list:
    state1 = phy_pro[phy_pro["state_name"] == state]
    state2 = phy_fin_mon[phy_fin_mon["state_name"] == state]
    district_list = state1["district_name"].unique().tolist()
    for district in district_list:
        district1 = state1[state1["district_name"] == district]
        district2 = state2[state2["district name"] == district]
        block_list = district1["block_name"].unique().tolist()
        for block in block_list:
            block1 = district1[district1["block_name"] == block]
            block2 = district2[district2["block name"] == block]
            df = pd.merge(
                left=block1,
                right=block2,
                how="left",
                left_on=["packages", "road name", "habitation name"],
                right_on=["package id", "road name", "habitation name"],
            )
            dframe.append(df)
frame = pd.concat(dframe, axis=0, ignore_index=True)
# Saving to CSV file
dest_path = project_folder + r"/data/processed/merged data"
print(dest_path)
if not os.path.isdir(dest_path):
    os.makedirs(dest_path)
frame.to_csv(dest_path + r"/merged_data.csv", index=False)
