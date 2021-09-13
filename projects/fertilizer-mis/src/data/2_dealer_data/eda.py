# importing packages
import pandas as pd
import pandas_profiling as pp

df = pd.read_csv(r"C:\Users\91987\Downloads\python\bipp-datasets\projects\fertilizer-mis\data\interim\dealer_data.csv")

profile = pp.ProfileReport(df)
profile.to_file("output.html")