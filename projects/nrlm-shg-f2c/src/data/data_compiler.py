from pathlib import Path

import pandas as pd

# defining directories
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")

time_folders = list(raw_path.glob("*"))


def data_compiler():

    while True:

        for x in time_folders[1:]:

            try:

                # time_stamp=str(file.parents[3]).split("\\")[-1]

                final_file = interim_path.joinpath(f"{x},{x.stem}.csv")

                if not final_file.exists():

                    print(f"{x.stem} doesn't exist. Collecting files")

                    data_list = []

                    files = list(x.glob("*/*/*/*.csv"))

                    for file in files:
                        print(file)
                        df = pd.read_csv(file)

                        data_list.append(df)

                    data_final = pd.concat(data_list, axis=0)

                    data_final.to_csv(final_file, index=False)

                    print(f"Exported {x.stem} as CSV to interim directory")

                else:
                    print(f"{x.stem} file exists. Moving to next month")
                    continue

            except ValueError:
                print(
                    f"{x.stem} has no files. Catching ValueError and moving to next month"
                )

        break


data_compiler()
