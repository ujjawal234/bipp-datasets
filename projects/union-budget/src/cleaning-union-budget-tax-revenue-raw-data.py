import ub_reciepts_functions
from ub_reciepts_functions import data_cleaning_pipeline
from ub_reciepts_functions import split_variable_column
import numpy as np

import pandas as pd
import re
from pathlib import Path
import csv

DATA_DIR = Path(__file__).parents[1] / "data"
DEST_PATH = DATA_DIR / "interim" / "ub__last_4_tax_revenue_years.csv"

files = list((DATA_DIR / "raw" / "Tax revenue").glob("*.xlsx"))
xls = [pd.read_excel(f).dropna(axis=1, how="all").dropna(axis=0, how="all") for f in files]


cln_dfs = map(lambda df: df.pipe(data_cleaning_pipeline).melt(id_vars=['heads','subhead']), xls)

df = pd.concat(cln_dfs)


# fnl_df = remove_characters_from_columns(fnl_df)
df.to_csv(DEST_PATH, index=False, quoting= csv.QUOTE_NONNUMERIC)

