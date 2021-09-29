from pathlib import Path

import pandas as pd


class Consolidation_Script:
    name = "Consolidation_Script"
    project_dir = str(Path(__file__).resolve().parents[2])
    parent_folder = project_dir + "/data/processed/"

    project_dir_new = str(Path(__file__).resolve().parents[2])
    parent_folder_new = project_dir_new + "/data/processed/All_States/"

    def FileConsolidator(self):
        # This funtion contains logic to consilidated files of all the states into one file and saving final data as CSV file.
        state_path = self.parent_folder
        directory = state_path
        # path generator for all csv files in raw_dir
        filepath_list = Path(directory).glob("**/*.csv")
        df_final = pd.DataFrame()
        for files in filepath_list:
            # Reading files into a dataframe using pandas
            df = pd.read_csv(files)
            df_final = df_final.append(df)
        print(df_final)

        file_path = self.parent_folder_new
        file_name = "All_States" + ".csv"
        self.final_directory(file_path)
        df_final.to_csv(file_path + "/" + file_name, index=False)

    def final_directory(self, file_path):
        # # This function creates directory and appropriate file path to save the data.
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


def main():
    x = Consolidation_Script()
    x.FileConsolidator()


if __name__ == "__main__":
    main()
