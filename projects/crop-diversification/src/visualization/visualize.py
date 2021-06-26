import json
import os

import altair as alt
import geopandas as gpd
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_six = "ag_census_2015_2016/six_states_15_16.csv"
rel_path = os.path.abspath(path_ag + path_six)
non_crop_six = pd.read_csv(rel_path, index_col=0)
states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]

path_tehsil_shp = "Tehsil_2020_v2_topo.json"
rel_path_shp = os.path.abspath(path_ag + path_tehsil_shp)
json_file = gpd.read_file(rel_path_shp, driver="TopoJSON")
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
json_file.columns = json_file.columns.str.lower()
json_file["state"] = json_file["state"].str.lower()
json_file = json_file.applymap(lambda s: s.lower() if type(s) == str else s)
json_file_six = json_file[json_file["state"].isin(states_six)]
df_vis = non_crop_six[non_crop_six["lgd_code"].notna()]
df_vis_six = df_vis[df_vis["state"].isin(states_six)]

path_state_shp = "State_2020.json"
json_state = gpd.read_file(os.path.abspath(path_ag + path_state_shp), driver="TopoJSON")
json_state.rename(columns={"State_LGD": "state_lgd", "stname": "state"}, inplace=True)
json_state["id"] = json_state["state_lgd"]
json_state["state"] = json_state["state"].str.lower()
json_state_gdf = json_state.to_json()  # string datatype
json_features_state = json.loads(json_state_gdf)  # dict type
data_geo_state = alt.Data(values=json_features_state["features"])


def state_mp(state, color_col, color_title, tooltip):
    gdf_merged = json_file.merge(
        df_vis_six, left_on="id", right_on="lgd_code", how="inner"
    )  # geodataframe
    json_gdf = gdf_merged.to_json()  # string datatype
    json_features = json.loads(json_gdf)  # dict type
    # json_features['features] is list type
    data_geo = alt.Data(values=json_features["features"])

    # Add states layer
    state_shape = (
        alt.Chart(data_geo_state)
        .mark_geoshape(stroke="white", fillOpacity=0, strokeWidth=2)
        .encode()
        .properties(width=800, height=800)
    )
    # Add Base Layer
    base = (
        alt.Chart(data_geo, title="Visualisation for six states")
        .mark_geoshape(stroke="white", strokeWidth=1)
        .encode()
        .properties(width=800, height=800)
    )
    # Add Choropleth Layer
    choro = (
        alt.Chart(data_geo)
        .mark_geoshape(
            # fill='lightgray',
            stroke="white"
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
    return (state_shape + base + choro).configure_view(strokeWidth=1)


def main():
    fig = state_mp(
        state=states_six,
        color_col="properties.hold_no:Q",
        color_title="no. of holdings",
        tooltip=[
            "properties.state_x:N",
            "properties.district_x:N",
            "properties.hold_no:Q",
            "properties.hold_ar:Q",
        ],
    )
    st.title("AgCensus Visualisation:")
    # st.subheader("Analize data at various levels")
    st.altair_chart(fig, use_container_width=True)

    # st.sidebar.title("Agcensus variable analysis:")
    # st.markdown("This dashboard analysizes change for any agcensus feature for the year 2010-11 and 2015-16")
    # st.sidebar.markdown("This dashboard analysizes change for any agcensus feature for the year 2010-11 and 2015-16")


if __name__ == "__main__":
    main()
