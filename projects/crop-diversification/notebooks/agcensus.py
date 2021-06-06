# %%
import json
import os

# import zipfile
from glob import glob

import altair as alt

# import altair_viewer
import geopandas as gpd

# import matplotlib.pyplot as plt
# import numpy as np
import pandas as pd

alt.renderers.enable("altair_viewer")

file_path = "./data/interim/Ag_Census_2015_2016/Non_Crop_2015_2016/"
read_file = glob(os.path.join(file_path, "*.csv"))
combined = pd.concat((pd.read_csv(file) for file in read_file), ignore_index=True)
combined.columns = combined.columns.str.lower()
combined["state"] = combined["state"].str.title()

# # combined dimmension (510400 x 49)
combined_drop = combined.drop(["uqid", "size_class", "soc_grp", "unnamed: 0"], axis=1)
# combined_drop[combined_drop.columns[4:]] = combined_drop[combined_drop.columns[4:]].fillna(0)
# print(combined_drop.head())
states = combined_drop["state"].unique()

group_lgd = combined_drop.groupby(["lgd_code"], as_index=False).sum()
# print(group_lgd.head())

json_file = gpd.read_file(
    os.path.join("./data/interim/Ag_Census_2015_2016/Tehsil_2020_v2_topo.json"),
    driver="TopoJSON",
)
json_file.rename(
    columns={
        "Sb_Dt_LGD": "lgd_code",
        "State_UT_N": "state",
        "Dist_Name": "district",
        "Sub_Dist_N": "tehsil",
    },
    inplace=True,
)
json_file["id"] = json_file["lgd_code"]


def state_mp(state, color_col, color_title, tooltip):
    group_lgd_st = (
        combined_drop.loc[combined_drop["state"] == state]
        .groupby("lgd_code", as_index=False)
        .sum()
    )
    json_file_st = json_file.loc[json_file["state"] == state]
    gdf_merged_st = json_file_st.merge(
        group_lgd_st, left_on="id", right_on="lgd_code", how="inner"
    )  # geodataframe
    json_gdf_st = gdf_merged_st.to_json()  # string datatype
    json_features_st = json.loads(json_gdf_st)  # dict type
    data_geo_st = alt.Data(
        values=json_features_st["features"]
    )  # json_features['features] is list type

    # Add Base Layer
    base = (
        alt.Chart(data_geo_st, title=state)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode()
        .properties(width=800, height=800)
    )
    # Add Choropleth Layer
    choro = (
        alt.Chart(data_geo_st)
        .mark_geoshape(
            # fill='lightgray',
            stroke="black"
        )
        .encode(
            alt.Color(
                color_col,
                type="quantitative",
                scale=alt.Scale(scheme="yelloworangered"),
                title=color_title,
            ),
            tooltip=tooltip,
        )
    )
    #   alt.renderers.enable('mimebundle')
    return (base + choro).configure_view(strokeWidth=0)


fig = state_mp(
    state="Madhya Pradesh",
    color_col="properties.gca_tot:Q",
    color_title="gca tot",
    tooltip=["properties.gca_tot:Q", "properties.hold_ar:Q"],
)
fig.show()
# fig.save('madhyaP.html')

# %%
