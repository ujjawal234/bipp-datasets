import functions as f
import pandas as pd

df = pd.read_csv("./data/processed/distwise.csv")
dist = "District Name"
name = "Nashik"
f.finddis(df, dist, name)
