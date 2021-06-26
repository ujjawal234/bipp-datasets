from . import agcensus as ag

df_nc15_six = ag.combined_drop15.loc[ag.combined_drop15["state"].isin(ag.states_six)]
print(df_nc15_six)
