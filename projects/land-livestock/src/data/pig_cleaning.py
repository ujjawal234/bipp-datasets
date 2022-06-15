from pathlib import Path

import pandas as pd

dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")

df = pd.read_csv(Path.joinpath(interim_data_path, "pig.csv"))
# District-wise pig population in 2019


df.to_csv(Path.joinpath(processed_data_path, "pig.csv"), index=False)
