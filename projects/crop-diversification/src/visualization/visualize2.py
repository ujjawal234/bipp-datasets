import os

import altair as alt
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_nc_var = "ag_census_2015_2016/non_crop_variables_selected.xlsx"
path_six = "ag_census_2015_2016/mapped6_nc_15_16.csv"

states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]
states_drop = ["All six states"] + states_six

df_nc15 = pd.read_csv(
    os.path.abspath(path_ag + path_six), index_col=0
)  # ungrouped df for all states and mapped for six states
df_vis_six = df_nc15[df_nc15["state"].isin(states_six)]  # df for visualisation

tehsil_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/Tehsil_2020_v2_topo.json"
data_geo_t = alt.topo_feature(tehsil_url, "Tehsil_2020_v2_new")

state_url = (
    "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/State_2020.json"
)
data_geo_s = alt.topo_feature(state_url, "State_2020")

# select by state
select_state = st.selectbox("Select a State", states_drop)

# parameter selection
items = df_vis_six.columns
# df of selected variables for non crop
df_var_selected = pd.read_excel(os.path.abspath(path_ag + path_nc_var))
df_var_selected = df_var_selected.iloc[:, 0:2]
index = [10, 11, 12, 13, 18, 19, 20, 21, 35, 37, 48, 49, 50]
parameter = [df_var_selected["Variable Name"][i] for i in index]
description = [df_var_selected["Description"][i] for i in index]

# select parameter by parameter description
select_param = st.selectbox("select a parameter", description)

param_dict = dict(zip(parameter, description))
param_dict_rev = dict(zip(param_dict.values(), param_dict.keys()))
colors = [
    "yelloworangered",
    "yelloworangebrown",
    "yellowgreen",
    "yellowgreenblue",
    "redpurple",
    "purplered",
    "purpleblue",
    "purplebluegreen",
    "greys",
    "blues",
    "goldred",
    "goldorange",
    "goldgreen",
]
color_dict = dict(zip(parameter, colors))

if select_state == "All six states":
    if select_param:
        value = str(param_dict_rev[select_param]) + ":Q"
        color_scheme = color_dict[param_dict_rev[select_param]]
    # state
    state_layer = (
        alt.Chart(data_geo_s)
        .mark_geoshape(fillOpacity=0, stroke="white")
        .properties(width=800, height=800)
    )
    # tehsil
    tehsil_layer = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=select_param,
            ),
            tooltip=["state:O", "district:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "district", param_dict_rev[select_param]],
            ),
        )
        .properties(width=800, height=800)
    )
    # tooltip
    tooltip_layer = (
        alt.Chart(data_geo_t)
        .mark_geoshape(
            fillOpacity=0,
        )
        .encode(
            color=value,
            tooltip=["state:O", "district:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "district", param_dict_rev[select_param]],
            ),
        )
    )
    map = tehsil_layer + state_layer + tooltip_layer
else:
    if select_param:
        value = str(param_dict_rev[select_param]) + ":Q"
        color_scheme = color_dict[param_dict_rev[select_param]]
    map = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=select_param,
            ),
            tooltip=["state:O", "district:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six[df_vis_six["state"].isin([select_state])],
                key="lgd_code",
                fields=["state", "district", param_dict_rev[select_param]],
            )
            # ).transform_filter(
            #     alt.FieldEqualPredicate(
            #         field="properties.State_UT_N", equal=select.upper()
            #     )
        )
        .properties(width=800, height=800)
    )

st.title("AgCensus Visualisation:")
st.altair_chart(map, use_container_width=True)
