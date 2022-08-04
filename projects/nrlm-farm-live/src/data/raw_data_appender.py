from pathlib import Path

import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_path = dir_path.joinpath("data", "raw")
interim_path = dir_path.joinpath("data", "interim")

# raw files
raw_files = list(raw_path.glob("*"))

# calling in all the files


def data_plant_appender():
    for parent in raw_files[1:]:
        appended_file_path = interim_path.joinpath(
            f"{parent.stem.lower()}.csv"
        )

        if not appended_file_path.exists():

            files = list(parent.glob("*/*.csv"))
            print(parent)
            data_list = []
            for file in files:
                print(file)
                data = pd.read_csv(file)
                data_list.append(data)

            data_appended = pd.concat(data_list, axis=0)
            data_appended.to_csv(appended_file_path, index=False)

        else:
            print(parent.stem, "exists. Moving to next category")
            continue


data_plant_appender()
