import os
from pathlib import Path

import click
import numpy as np
import pandas as pd


# reading the data
class Consolidation_Script:
    name = "Consolidation_Script"
    # def __init__(self):
    project_dir = str(Path(__file__).resolve().parents[2])
    parent_folder = project_dir + "/data/raw/"

    project_dir_new = str(Path(__file__).resolve().parents[2])
    parent_folder_new = project_dir_new + "/data/processed/"
    # print(parent_folder)

    def FileConsolidator(self, state_name, year_of_data):
        # This function combines all individual ac_files into single file for the states of Odisha, Tripura, Nagaland and Meghalaya.
        state_path = self.parent_folder + "/" + str(state_name)
        directory = state_path
        # path generator for all csv files in raw_dir
        filepath_list = Path(directory).glob("**/*.csv")
        df_combined = pd.DataFrame()
        for files in filepath_list:
            print(files)
            # Extracting ac_names from the absolute file-path
            ac_name = os.path.basename(files).split(".")[0]
            # print(ac_name)
            #   Reading files into a dataframe using pandas
            df = pd.read_csv(files)
            df["AC"] = ac_name
            df["State"] = state_name
            df["Year"] = year_of_data
            df.index = np.arange(1, len(df) + 1)
            df["Polling_Station_Number"] = df.index
            # df_combined=pd.concat(df_combined,df,ignore_index=True)
            df_combined = df_combined.append(df)
            df_combined = df_combined[
                [
                    "Year",
                    "State",
                    "AC",
                    "Polling_Station_Number",
                    "Polling_Station_Name",
                ]
            ]
        print(df_combined)

        file_path = self.parent_folder_new + "/" + state_name
        file_name = state_name + ".csv"
        self.final_directory(file_path)
        df_combined.to_csv(file_path + "/" + file_name, index=False)

    def final_directory(self, file_path):
        # This function creates directory and appropriate file path to save the data.
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


@click.command()
@click.option("-state", default="2_Tripura", help="State_Name")
@click.option(
    "-year", default="2018", help="Provide Year in which Data was collected"
)
def main(state, year):
    x = Consolidation_Script()
    x.FileConsolidator(state_name=state, year_of_data=year)


if __name__ == "__main__":
    main()
