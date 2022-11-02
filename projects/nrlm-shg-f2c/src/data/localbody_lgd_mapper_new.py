from pathlib import Path

import pandas as pd
from fuzzywuzzy import process

# defining directories
dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = dir_path.joinpath("data", "external")

# change in lgd file name accordingly
lgd_in_file = external_path.joinpath("all_pri.csv")

# change in file name accordingly
in_file = interim_path.joinpath("2022_23_June_block_lgd_mapped.csv")

# path to hold iterated files produced during LGD WIP
lgd_iter = interim_path.joinpath("lgd_iter")
if not lgd_iter.exists():
    lgd_iter.mkdir(parents=True)
# lgd_iter_file=lgd_iter.joinpath("lgd_iter_file.csv")

# path to hold mapped state files.
mapped_folder_path = interim_path.joinpath("localbody_mapped")
if not mapped_folder_path.exists():
    mapped_folder_path.mkdir(parents=True)


def lgd_master():
    def lgd_file_prep():
        # Importing LGD mapping index
        lgd = pd.read_csv(lgd_in_file, encoding="ISO8859")
        # lgd.drop("St_Cs2011_code", axis=1, inplace=True)

        # renaming the concerned columns
        lgd = lgd.rename(
            columns={
                "state_name": "state",
                # "district_name": "district",
                # "block_name": "block",
                "local_body_name": "localbody",
            }
        )

        lgd["state"] = lgd["state"].str.upper()
        # lgd['district']=lgd['district'].str.upper()
        # lgd['block']=lgd['block'].str.upper()
        lgd["localbody"] = lgd["localbody"].str.upper()

        # concatenating states, districts, blocks and gram panchayats(Local Body)
        # lgd["state_dist"] = lgd["state"] + lgd["district"]
        # lgd["state_dist_block"] = lgd["state"] + lgd["district"] + lgd["block"]
        # lgd["state_dist_block_localbody"] = lgd["state"] + lgd["district"] + lgd["block"] + lgd["localbody"]
        lgd["state_localbody"] = lgd["state"] + lgd["localbody"]

        # print(lgd.columns)
        print("LGD file has been prepared")

        return lgd

    def data_file_prep():

        # importing the interim file
        df = pd.read_csv(in_file)

        # uppercasing the local body names in state files
        df["localbody"] = df["localbody"].str.replace("_", " ").str.upper()
        df["block"] = df["block"].str.replace("_", " ").str.upper()
        df["state"] = df["state"].str.replace("_", " ").str.upper()
        df["district"] = df["district"].str.replace("_", " ").str.upper()

        print("Data file has been prepared")

        return df

    def gp_name_clean():
        df = data_file_prep()
        #     conditions=[
        #         #ARUNACHAL PRADESH
        #         (df['state'] == "ARUNACHAL PRADESH") &  (df["district"] == "LOWER SUBANSIRI") & (df['block'] == "HONG-HARI") & (df['localbody'] == "SICHUSII"),

        #         #CHHATTISGARH
        #         (df['state'] == "CHHATTISGARH") &  (df["district"] == "BEMETARA") & (df['block'] == "SAJA") & (df['localbody'] == "CHEEJGAON"),
        #         (df['state'] == "CHHATTISGARH") &  (df["district"] == "BILASPUR") & (df['block'] == "MASTURI") & (df['localbody'] == "DEWREE"),
        #         (df['state'] == "CHHATTISGARH") &  (df["district"] == "BILASPUR") & (df['block'] == "MASTURI") & (df['localbody'] == "DHURVAKARI"),

        #         #GOA
        #         (df['state'] == "GOA") &  (df["district"] == "SOUTH GOA") & (df['block'] == "QUEPEM") & (df['localbody'] == "CAVOREM-PIRLA"),

        #         #HIMACHAL PRADESH
        #         (df['state'] == "HIMACHAL PRADESH") &  (df["district"] == "MANDI") & (df['block'] == "MANDI SADAR") & (df['localbody'] == "BEER"),
        #         #(df['state'] == "HIMACHAL PRADESH") &  (df["district"] == "SHIMLA") & (df['block'] == "MASHOBRA") & (df['localbody'] == "BATHMANA JABRI"),
        #         #(df['state'] == "HIMACHAL PRADESH") &  (df["district"] == "SOLAN") & (df['block'] == "DHARAMPUR") & (df['localbody'] == "BUGHAR KANAITA"),

        #         #KARNATAKA
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MANDI") & (df['block'] == "MANDI SADAR") & (df['localbody'] == "BEER"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "BELAGAVI") & (df['block'] == "BYLAHONGAL") & (df['localbody'] == "MARADINAGALAPUR"),
        #         #(df['state'] == "KARNATAKA") &  (df["district"] == "BENGALURU RURAL") & (df['block'] == "NELAMANGALA") & (df['localbody'] == "ARISHINAKUNTE"),
        #         #(df['state'] == "KARNATAKA") &  (df["district"] == "BENGALURU RURAL") & (df['block'] == "NELAMANGALA") & (df['localbody'] == "BASAVANAHALLI"),
        #         #(df['state'] == "KARNATAKA") &  (df["district"] == "BENGALURU RURAL") & (df['block'] == "NELAMANGALA") & (df['localbody'] == "VISHWESHWARAPURA"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "CHAMARAJANAGARA") & (df['block'] == "HANURU") & (df['localbody'] == "SHAAGYA"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "CHITRADURGA") & (df['block'] == "CHALLAKERE") & (df['localbody'] == "THIMMANNANAYAKANAKOTE"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "CHITRADURGA") & (df['block'] == "HIRIYUR") & (df['localbody'] == "VANIVILASAPURA"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "DAKSHINA KANNADA") & (df['block'] == "BANTVAL") & (df['localbody'] == "BALEPUNI"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "DAKSHINA KANNADA") & (df['block'] == "BANTVAL") & (df['localbody'] == "KURNAD"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "DAKSHINA KANNADA") & (df['block'] == "MANGALURU") & (df['localbody'] == "ATHIKARIBETTU"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "HASSAN") & (df['block'] == "ARSIKERE") & (df['localbody'] == "J.C.PUR"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MANDYA") & (df['block'] == "MANDYA") & (df['localbody'] == "MANDYA GRAMANATHARA"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MANDYA") & (df['block'] == "PANDAVAPURA") & (df['localbody'] == "KURUBARABETTAHALLI"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MANDYA") & (df['block'] == "PANDAVAPURA") & (df['localbody'] == "KURUBARABETTAHALLI"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MANDYA") & (df['block'] == "PANDAVAPURA") & (df['localbody'] == "TIRUMALASAGARA"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MYSURU") & (df['block'] == "HUNSUR") & (df['localbody'] == "UDBOORKAVAL"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "MYSURU") & (df['block'] == "SARAGURU") & (df['localbody'] == "KALLAMBALU"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "RAICHUR") & (df['block'] == "MANVI") & (df['localbody'] == "BAYAGWAT"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "RAMANAGARA") & (df['block'] == "RAMANAGARA") & (df['localbody'] == "HULIKERE GUNNUR"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "UTTARA KANNADA") & (df['block'] == "HONAVAR") & (df['localbody'] == "HALEMATH"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "UTTARA KANNADA") & (df['block'] == "HONAVAR") & (df['localbody'] == "MANKI GULADKERI"),
        #         (df['state'] == "KARNATAKA") &  (df["district"] == "VIJAYAPURA") & (df['block'] == "VIJAYAPURA") & (df['localbody'] == "LOHGAON"),

        #    ]

        #     options=[
        #         #ARUNACHAL PRADESH
        #         "SIIRO SANGO",

        #         #CHHATTISGARH
        #         "CHEEJGAON",
        #         "DEVARI",
        #         "DHRUVKIRARI",

        #         #GOA
        #         "CAUREM",

        #         #HIMACHAL PRADESH
        #         "BIR (REW)",

        #         #KARNATAKA
        #         "M. NAGALAPUR",

        #         #TAMIL NADU
        #     ]

        #     df["localbody"] = np.select(
        #         conditions, options, default=df["localbody"]
        #     )

        return df

    def data_name_concater():
        df = gp_name_clean()

        # concatenating states, districts, blocks and localbody for states
        # df["state_dist"] = df["state"] + df["district"]
        # df["state_dist_block"] = df["state"] + df["district"] + df["block"]
        df["state_localbody"] = df["state"] + df["localbody"]

        return df

    def lgd_mapper():

        lgd = lgd_file_prep()

        df = data_name_concater()

        # lgd=lgd.iloc[:,[0,1,2,3,6]]  #to be applied to remove localbody name columns while iterating over blocks
        lgd.drop_duplicates("state_localbody", inplace=True)

        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_localbody",
            right_on="state_localbody",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        print(df1.columns)

        tabs = df1["_merge"].value_counts()

        print(tabs)

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state_data",
                "district",
                "block",
                "localbody_data",
                # "state_dist_block",
                "state_localbody",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(
            subset="state_localbody"
        )

        return [not_lgd_mapped, lgd, df]

    def fuzzy_mapper():

        output = lgd_mapper()
        df = output[2]
        state_list = df["state"].unique()
        # print(state_list)
        for state in state_list:
            not_lgd_mapped = output[0]
            lgd = output[1]

            # defining state wise lgd_iter_file path and mnapped file path
            lgd_iter_file = lgd_iter.joinpath(f"{state}_lgd_iter_file.csv")

            mapped_file_path = mapped_folder_path.joinpath(
                f"2022_23_June_{state}_localbody_lgd_mapped"
            )

            if not lgd_iter_file.exists():
                if state not in (["MEGHALAYA", "MIZORAM", "NAGALAND"]):

                    # Filtering df, lgd and not_lgd_mapped for each state
                    df_proxy = df[df["state"] == state]
                    not_lgd_mapped = not_lgd_mapped[
                        not_lgd_mapped["state_data"] == state
                    ]
                    # print(not_lgd_mapped)
                    # print(lgd['state'].unique())
                    lgd = lgd[lgd["state"] == state]
                    # print(lgd)
                    # Fuzzywuzzy for mapping
                    print(
                        f"******* {state} - Initiating Fuzzy Mapping ********"
                    )

                    # creating a list of matches for unmerged state_dist_block_localbody names
                    result = [
                        process.extractOne(i, lgd["state_localbody"])
                        for i in not_lgd_mapped["state_localbody"]
                    ]

                    # print(result)
                    # print(len(result))

                    # converting and editing the fuzzy matches to a dataframe
                    result = pd.DataFrame(
                        result, columns=["match", "score", "id"]
                    )
                    result.drop("id", axis=1, inplace=True)

                    # creating a proxy dataframe with names of unmerged original names of state_dist_block_localbody
                    not_lgd_proxy_df = (
                        pd.DataFrame(
                            not_lgd_mapped["state_localbody"], index=None
                        )
                        .reset_index()
                        .drop("index", axis=1)
                    )

                    # creating a dataframe for fuzzy mapper
                    mapper_df = pd.concat(
                        [not_lgd_proxy_df, result],
                        axis=1,
                        ignore_index=True,
                        names=["original", "match", "score"],
                    )
                    mapper_df = mapper_df[mapper_df[2] >= 90]

                    # creating a dictionary for fuzzy mapper
                    mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))

                    # applying the mapper dictionary on the original processed state file to correct for fuzzy matched names
                    df_proxy["state_localbody"] = df_proxy[
                        "state_localbody"
                    ].replace(mapper_dict)

                    print(
                        "*******Fuzzy Mapping for has ended. Proceeding for second round of Data Merge*******",
                    )

                    # Second round of data merge with update fuzzy matched names along with exact original names in the state file
                    # Merging lgd and state file
                    df1 = pd.merge(
                        df_proxy,
                        lgd,
                        how="outer",
                        left_on="state_localbody",
                        right_on="state_localbody",
                        validate="m:1",
                        indicator=True,
                        suffixes=["_data", "_LGD"],
                    )

                    df1["_merge"].value_counts()

                    # filtering fuzzy unmapped observations
                    not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
                        [
                            "state_data",
                            "district",
                            "block",
                            "localbody_data",
                            # "state_dist_block",
                            "state_localbody",
                        ]
                    ]

                    not_lgd_mapped = not_lgd_mapped.drop_duplicates(
                        subset="state_localbody"
                    )

                    tabs = df1["_merge"].value_counts()

                    print(tabs)

                    df1 = df1.rename(
                        columns={
                            # 'State Code':'state_lgd_code',
                            "state_data": "state",
                            # 'District Code':"district_lgd_code",
                            # 'district_data':"district",
                            # ' Block Code':"block_lgd_code",
                            # 'block_LGD':"block",
                            "local_body_code": "localbody_lgd_code",
                            "localbody_LGD": "localbody",
                            # 'village_name_data':'village_name'
                        }
                    )

                    df1 = df1[df1["_merge"] == "both"]

                    final_col_list = [
                        "year",
                        "month",
                        "state_lgd_code",
                        "state",
                        "district_lgd_code",
                        "district",
                        "block_lgd_code",
                        "block",
                        "localbody_lgd_code",
                        "localbody",
                        "village_name",
                        "num_new_shg",
                        "amt_new_shg",
                        "num_pre_nrlm_revived_shg",
                        "amt__pre_nrlm_revived_shg",
                        "total_num_shg",
                        "total_amt_shg",
                    ]

                    df1 = df1[final_col_list]

                    # print("Exporting not_lgd and LGD matched as CSV file to the Interim sub directory")
                    not_lgd_mapped.to_csv(lgd_iter_file, index=False)
                    lgd.to_csv(str(lgd_iter) + "/lgd.csv", index=False)
                    df1.to_csv(mapped_file_path, index=False)

                    continue

            else:
                print(
                    f"Fuzzy Mapping for {state} is done. Please delete the concerned lgd iter file to rerun mapping."
                )

    fuzzy_mapper()

    return


lgd_master()
