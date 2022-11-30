from pathlib import Path

import numpy as np
import pandas as pd

# defining paths
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", "google_mobility.csv")


df = pd.read_csv(raw_path)
# print(df['location_key'].str.contains("IN_.*"))

df_new = df[df["location_key"].str.contains("IN_.*", regex=True, na=np.nan)]
print(df_new)
# # print(df.columns)
# print(list(df["location_key"].unique()))


# loc_key_in=[x.match(r"IN_.*") for x in loc_key]

# print(loc_key_in)
