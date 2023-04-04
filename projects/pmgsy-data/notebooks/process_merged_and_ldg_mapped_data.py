"""
Vanshika had prepared an lgd mapped dataset that was created by merging the financial progress report and the physical progress report.
She had this dataset stored in a 'completed.csv' file.

This script starts from that 'completed.csv' file and prepares interim and processed datasets that are ready for IDP 2.0 upload.
Please note that the final processed data created by this script is not perfect.
"""
# %%
import pandas as pd
from pathlib import Path
# %%
datadir = Path.cwd().parent / "data"
fnl_data_dir = datadir / "processed"
interim_data_dir = datadir / "interim"
# %%
ttl_df = pd.read_csv(str(Path.cwd().parent / "complete_total.csv"))
df = ttl_df.copy()
# %%
df.columns = df.columns.str.replace(
    "_x", "_physical").str.replace("_y", "_financial")
# %%
df["sanctioned year_financial"] = df["sanctioned year_financial"].str.replace(
    r"(\d{4})\s-\s(\d{4})", r"\1-\2", regex=True)
# %%
df = df.drop(columns=["state name", "district name", "block name",
                      "package id", "block_name_z", "state_name_z", "district_name_z", "uqid1", "id_physical", "id_financial"])
# %%
df.columns = df.columns.str.replace(" ", "_")
# %%
df.to_csv(str(interim_data_dir / "pmgsy.csv"), index=False)
df.to_parquet(str(interim_data_dir / "pmgsy.parquet"))
# %%
#%%
fnl_df = df.copy()
# %%
fnl_df.columns = fnl_df.columns.str.replace(".", "", regex=False)
fnl_df.columns = fnl_df.columns.str.replace("cd", "cross_drainage")
fnl_df.columns = fnl_df.columns.str.replace("lsb", "long_span_bridge")
fnl_df.rename(columns={
    "upgrade_/_new": "upgrade_or_new",
    
}, inplace=True)
#%%
fnl_df = fnl_df.drop(columns=["sanctioned_year_physical", "sanctioned_year_financial", "srno_physical", "srno_financial"])
# %%
fnl_df = fnl_df.drop(columns=["habitation_name"]).drop_duplicates()
#%%
fnl_df.to_csv(str(fnl_data_dir / "pmgsy.csv"), index=False)
fnl_df.to_parquet(str(fnl_data_dir / "pmgsy.parquet"))
