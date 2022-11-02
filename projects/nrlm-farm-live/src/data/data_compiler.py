import re
from pathlib import Path

import numpy as np
import pandas as pd

# defining directories
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")
final_file = interim_path.joinpath("fl2_appended.csv")

files = list(raw_path.glob("*/*/*/*/*.csv"))


def data_compiler():

    if not final_file.exists():
        data_list = []

        for file in files:
            df = pd.read_csv(file)

            # removing indicator_number column
            df.drop("indicator_number", axis=1, inplace=True)

            # corecting geography names
            for col in df.columns[2:5]:
                df[col] = df[col].str.upper()

            def column_name_rectifier():

                col_names = [
                    "year",
                    "month",
                    "state",
                    "district",
                    "block",
                    "indicators",
                    "purpose",
                    "as_on_march",
                    "financial_year_target",
                    "num_current_month",
                    "total",
                    "percentage_of_achievement",
                    "cumulative_achievement",
                ]
                # Some datasets have 13 column ie without month value column while some have 14 column ie with month value
                # see 2020-21 AP Anantpur Anantpur &
                #    2020-21 AP Anantpur Atmakur
                # For 14 columns file, the month value column will be renamed to "num_current_month"
                # For 13 column data, a new column with the same name will be added

                if len(df.columns) == 12:
                    df.insert(10, "num_current_month", np.nan)
                    df.columns = col_names

                elif len(df.columns) == 13:
                    df.columns = col_names

                else:
                    print(df.iloc[0, 0:5], "doesnt have 12 or 13 columns.")
                    print("subsetting unnecessary columns")
                    print(f"Month column needed is {df['month'][0].lower()}")
                    proxy_cols_names = [
                        "year",
                        "month",
                        "state",
                        "district",
                        "block",
                        "indicators",
                        "purpose",
                        str(
                            "as_on_march_"
                            + df["year"][0]
                            .replace("-2021", "")
                            .replace("-2022", "")
                            .replace("-", "")
                        ),
                        str(
                            "financial_year_"
                            + df["year"][0].replace("-20", "_")
                            + "_target"
                        ),
                        "total",
                        "percentage_of__achievement",
                        "cumulative_achievement",
                    ]
                    proxy_cols_names.append(df["month"][0].lower())

                    drop_col_names = [
                        x for x in df.columns if x not in proxy_cols_names
                    ]

                    # print(proxy_cols_names)
                    # print(drop_col_names)
                    df.drop(drop_col_names, axis=1, inplace=True)
                    # print(df.columns)
                    df.columns = col_names

                return df

            # def nan_creator(): #function to replace 0 in columns as NaN and filter out rows with only NaN
            #     df=column_name_rectifier()
            #     df.replace(0, np.nan, inplace=True)
            #     df.dropna(subset=['as_on_march', 'financial_year_target', "num_current_month", 'total',
            #         'percentage_of_achievement', 'cumulative_achievement'],axis=0, how="all", inplace=True)

            #     return df

            def melter():
                df = column_name_rectifier()

                melt_col_list = [
                    "as_on_march",
                    "financial_year_target",
                    "num_current_month",
                    "total",
                    "percentage_of_achievement",
                    "cumulative_achievement",
                ]

                placeholder = []

                for i in melt_col_list:

                    df1 = df.pivot(
                        index=["year", "month", "state", "district", "block"],
                        columns=["indicators", "purpose"],
                        values=i,
                    )

                    col1 = [
                        "_".join(
                            re.sub("[^a-zA-Z]+", " ", col.lower()).split(" ")[
                                :4
                            ]
                        )
                        for col in df1.columns.get_level_values(0)
                    ]
                    col2 = [
                        re.sub("[^a-zA-Z]+", " ", col.lower()).replace(
                            " ", "_"
                        )
                        for col in df1.columns.get_level_values(1)
                    ]

                    df1 = df1.reset_index()
                    df1.columns = [
                        "year",
                        "month",
                        "state",
                        "district",
                        "block",
                    ] + [i[0] + "_" + i[1] for i in zip(col1, col2)]

                    df1.insert(5, "value_type", i)

                    placeholder.append(df1)

                    # print(df.columns)

                df_final = pd.concat(placeholder, axis=0)

                col_list = df_final.columns
                # col_list=[re.sub("[^a-z_]", "", x.lower().strip().replace(" ","_").replace("__","_")) for x in col_list]
                df_final.columns = col_list

                return df_final

            data_list.append(melter())

        data_final = pd.concat(data_list, axis=0)

        data_final.to_csv(final_file, index=False)

    else:
        print("The appended file exists.")
        print(
            "Renaming certain column names which became duplicates becuase of a bug in the process."
        )

        df = pd.read_csv(final_file)
        df = df.rename(
            columns={
                "no_of_local_groups_oth.1": "no_of_local_groups_reg_pgs_portal_oth",
                "no_of_mahila_kisan_vd.1": "no_of_mahila_kisan_hh_agri_garden_vd",
                "no_of_mahila_kisans_vd.1": "no_of_mahila_kisans_shareholders_vd",
            }
        )

        # Overwriting existing file
        df.to_csv(final_file, index=False)


data_compiler()
