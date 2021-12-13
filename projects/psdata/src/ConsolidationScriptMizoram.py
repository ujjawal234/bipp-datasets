from pathlib import Path

import click
import numpy as np
import pandas as pd


# reading the data
class Consolidation_Script:

    name = "Consolidation_Script"

    project_dir = str(Path(__file__).resolve().parents[1])
    parent_folder = project_dir + "/data/raw/"

    project_dir_new = str(Path(__file__).resolve().parents[1])
    parent_folder_new = project_dir_new + "/data/processed/"
    # print(parent_folder)

    def FileConsolidator(self, state_name):

        df_combined = pd.DataFrame()
        df_combined = pd.read_csv(
            r"C:\Users\kriti\bipp-datasets\projects\psdata\data\raw\8_Mizoram\Final_ps_data_Mizoram.csv"
        )
        df_combined["State"] = state_name
        df_combined.index = np.arange(1, len(df_combined) + 1)
        df_combined.drop(labels=["District"], axis=1, inplace=True)
        df_combined.rename(
            columns={
                "Polling Station No.": "Polling_Station_Number",
                "Polling Station Name": "Polling_Station_Name",
            },
            inplace=True,
        )
        df_combined = df_combined[
            ["Year", "State", "AC", "Polling_Station_Number", "Polling_Station_Name"]
        ]
        print(df_combined)

        file_path = self.parent_folder_new + state_name
        print(file_path)
        file_name = state_name + ".csv"
        self.final_directory(file_path)
        df_combined.to_csv(file_path + "/" + file_name, index=False)

    def final_directory(self, file_path):
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)

    # logic to consolidate all the files should come here


@click.command()
@click.option("-state", default="1_WestBengal", help="State_Name")
def main(state):
    x = Consolidation_Script()
    x.FileConsolidator(state_name=state)


if __name__ == "__main__":
    main()
