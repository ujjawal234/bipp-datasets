import pandas as pd

# Reading Files and assigining variables, grouping data
df = pd.read_csv("./data/raw/final_agcensus.csv")
state_grp = df.groupby(["state_name"])
dist_grp = df.groupby(["district_name"])
tehsil_grp = df.groupby(["tehsil_name"])

# Getting Variables from users
state = input("Input State Name")
district = input("Input District Name")

# Aggregating data on the basis of state, district and tehsil
state_data = (
    state_grp.get_group(state)
    .groupby(["state_name", "social_group"])
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
district_data = (
    dist_grp.get_group(district)
    .groupby(["district_name", "social_group"])
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
tehsil_data = (
    dist_grp.get_group(district)
    .groupby(["tehsil_name", "social_group"])
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)

# Printing final dataframe with all the values
final = pd.concat([state_data, district_data, tehsil_data], axis=1)
print(final)

# Asking user for saving file
response = input("Want to save results to CSV")
if response == "Y" or "y":
    response2 = input("Type file name")
    final.to_csv("./data/processed/%s.csv" % (response2))
elif response == "N" or "n":
    print("Not saving the data")
else:
    print("Please input the correct reponse")
