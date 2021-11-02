import pandas as pd

df = pd.read_csv("./data/raw/cleaned.csv")
data = pd.DataFrame()
ql = ["district_name", "tehsil_name"]
# Creating Variables having one social class wise data each.
d = (
    df[df["social_group"] == "Scheduled Caste"]
    .groupby(ql)
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
sc = (
    df[df["social_group"] == "Scheduled Caste"]
    .groupby("tehsil_name")
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
st = (
    df[df["social_group"] == "Scheduled Tribe"]
    .groupby("tehsil_name")
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
sts = (
    df[df["social_group"] == "Scheduled Tribes"]
    .groupby("tehsil_name")
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
ins = (
    df[df["social_group"] == "Institutional"]
    .groupby("tehsil_name")
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)
oth = (
    df[df["social_group"] == "Others"]
    .groupby("tehsil_name")
    .agg({"gca_unirr_ar_state": "sum"})
    .reset_index()
)

# Combining two social class data
finalst = st["gca_unirr_ar_state"] + sts["gca_unirr_ar_state"]

# Appending Columns into new dataframe
data[["District Name", "Tehsil Name"]] = d[["district_name", "tehsil_name"]]
data["Schedule Tribes"] = finalst
data["Schedule Caste"] = sc["gca_unirr_ar_state"]
data["Institutional"] = ins["gca_unirr_ar_state"]
data["Others"] = oth["gca_unirr_ar_state"]

# Finding District
dist = input("Please enter district name")
query2 = data[data["District Name"] == dist]
