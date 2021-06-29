import altair as alt
import streamlit as st
from visualize import df_vis_six, states_drop

path_ag = "./data/interim/agcensus_isb/"

tehsil_url = "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/Tehsil_2020_v2_topo.json"
data_geo_t = alt.topo_feature(tehsil_url, "Tehsil_2020_v2_new")

state_url = (
    "https://raw.githubusercontent.com/Shahbaz67/bipp_personal/main/State_2020.json"
)
data_geo_s = alt.topo_feature(state_url, "State_2020")

select = st.selectbox("Select a State", states_drop)
if select == "All six states":
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
                "hold_no:Q",
                type="quantitative",
                scale=alt.Scale(scheme="yelloworangered"),
                title="no. of holdings",
            ),
            tooltip=["state:O", "district:O", "hold_no:Q", "hold_ar:Q"],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "district", "hold_no", "hold_ar"],
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
            color="hold_no:Q",
            tooltip=["state:O", "district:O", "hold_no:Q", "hold_ar:Q"],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six,
                key="lgd_code",
                fields=["state", "district", "hold_no", "hold_ar"],
            ),
        )
    )
    map = tehsil_layer + state_layer + tooltip_layer
else:
    map = (
        alt.Chart(data_geo_t)
        .mark_geoshape(stroke="black", strokeWidth=1)
        .encode(
            color=alt.Color(
                "hold_no:Q",
                type="quantitative",
                scale=alt.Scale(scheme="yelloworangered"),
                title="no. of holdings",
            ),
            tooltip=["state:O", "district:O", "hold_no:Q", "hold_ar:Q"],
        )
        .transform_lookup(
            lookup="properties.Sb_Dt_LGD",
            from_=alt.LookupData(
                data=df_vis_six[df_vis_six["state"].isin([select])],
                key="lgd_code",
                fields=["state", "district", "hold_no", "hold_ar"],
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
