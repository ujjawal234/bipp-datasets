import os

import altair as alt
import pandas as pd
import streamlit as st

path_ag = "./data/interim/agcensus_isb/"
path_nc_var = "ag_census_2015_2016/non_crop_variables_selected.xlsx"
path_nc_ungr = "ag_census_2015_2016/combined6_mapped_nc_ungr_15_16.csv"
path_c_ungr = "ag_census_2015_2016/combined6_mapped_c_ungr_15_16.csv"

states_six = [
    "punjab",
    "maharashtra",
    "jharkhand",
    "chhattisgarh",
    "odisha",
    "karnataka",
]
states_drop = ["All six states"] + states_six

df_nc_ungr_nc15 = pd.read_csv(os.path.abspath(path_ag + path_nc_ungr))
df_c_ungr_nc15 = pd.read_csv(os.path.abspath(path_ag + path_c_ungr))

tehsil_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/Tehsil_2020_v2_topo.json"
data_geo_t = alt.topo_feature(tehsil_url, "Tehsil_2020_v2_new")

state_url = (
    "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/State_2020.json"
)
data_geo_s = alt.topo_feature(state_url, "State_2020")

st.title("AgCensus Visualisation:")
st.sidebar.title("Filter data")

# select by social group (crop & non crop)
soc_gr = df_nc_ungr_nc15["soc_grp"].unique()
myorder_soc = [4, 0, 1, 3, 2]
soc_gr = x = [soc_gr[i] for i in myorder_soc]

# select by size class (crop & non crop)
size_class = df_nc_ungr_nc15["size_class"].unique()
myorder_size = [15, 2, 4, 7, 11, 14]
size_class = [size_class[i] for i in myorder_size]

# select parameter by parameter description (non crop only)
# df of selected variables for non crop
df_var_selected = pd.read_excel(os.path.abspath(path_ag + path_nc_var))
df_var_selected = df_var_selected.iloc[:, 0:2]
index = [10, 11, 12, 13, 18, 19, 20, 21, 35, 37, 48, 49, 50]

parameter = [df_var_selected["Variable Name"][i] for i in index]
description = [df_var_selected["Description"][i] for i in index]
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

# select a parameter by parameter description (crop only)
parameter_crop = ["no_hold", "irr_ar", "unirr_ar", "total"]
description_crop = [
    "Total Holdings Number",
    "Irrigated Area",
    "Unirrigated Area",
    "Total Area",
]

param_dict_crop = dict(zip(parameter_crop, description_crop))
param_dict_rev_crop = dict(zip(param_dict_crop.values(), param_dict_crop.keys()))
colors_crop = ["yelloworangered", "goldred", "goldorange", "goldgreen"]
color_dict_crop = dict(zip(parameter_crop, colors_crop))

# Filter options
select_state = st.selectbox("Select a State", states_drop)
select_crop = st.sidebar.selectbox("Crop Type", ["Crop", "Non-Crop"])
select_soc_grp = st.sidebar.radio("Pick a social group", soc_gr)
select_size_class = st.sidebar.radio("Pick a size class", size_class)

# Dashboard setup
if select_crop == "Non-Crop":
    select_param = st.selectbox("select a parameter for non-crop", description)
    value = str(param_dict_rev[select_param]) + ":Q"
    color_scheme = color_dict[param_dict_rev[select_param]]
    df = df_nc_ungr_nc15
    field = param_dict_rev
else:
    select_param = st.selectbox("select a parameter for crop", description_crop)
    value = str(param_dict_rev_crop[select_param]) + ":Q"
    color_scheme = color_dict_crop[param_dict_rev_crop[select_param]]
    df = df_c_ungr_nc15
    field = param_dict_rev_crop

# select_state, select_param = st.beta_columns([1,1])


@st.cache
def load_data(data, state):
    return data[data["state"] == state]


@st.cache
def filter_data(data, soc, size):
    return data[(data["soc_grp"] == soc) & (data["size_class"] == size)]


if select_state == "All six states":
    df_vis_six = filter_data(df, select_soc_grp, select_size_class)
    # state
    state_layer = (
        alt.Chart(data_geo_s)
        .mark_geoshape(fillOpacity=0, stroke="white")
        .properties(width=800, height=800)
    )
    alt.data_transformers.disable_max_rows()

    # tehsil
    tehsil_layer = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=field[select_param],
            ),
            tooltip=["state:O", "tehsil:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "tehsil", field[select_param]],
            ),
        )
        .properties(width=800, height=800)
    )
    alt.data_transformers.disable_max_rows()

    # tooltip
    tooltip_layer = (
        alt.Chart(data_geo_t)
        .mark_geoshape(
            fillOpacity=0,
        )
        .encode(
            color=value,
            tooltip=["state:O", "tehsil:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "tehsil", field[select_param]],
            ),
        )
    )
    alt.data_transformers.disable_max_rows()
    map = tehsil_layer + state_layer + tooltip_layer
else:
    df_vis_six = load_data(df, select_state)
    df_vis_six = filter_data(df_vis_six, select_soc_grp, select_size_class)
    map = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                value,
                type="quantitative",
                scale=alt.Scale(scheme=color_scheme),
                title=field[select_param],
            ),
            tooltip=["state:O", "tehsil:O", value],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "tehsil", field[select_param]],
            )
            # ).transform_filter(
            #     alt.FieldEqualPredicate(
            #         field="properties.State_UT_N", equal=select.upper()
            #     )
        )
        .properties(width=800, height=800)
    )
    alt.data_transformers.disable_max_rows()

st.altair_chart(map, use_container_width=True)
