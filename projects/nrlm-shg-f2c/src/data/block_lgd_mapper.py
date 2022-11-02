from pathlib import Path

import numpy as np
import pandas as pd
from fuzzywuzzy import process

# defining directories
dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = dir_path.joinpath("data", "external")

# change in lgd file name accordingly
lgd_in_file = external_path.joinpath("Blocks.csv")

# change in file name accordingly
in_file = interim_path.joinpath("2022_23_June_district_lgd_mapped.csv")

# path to hold iterated files produced during LGD WIP
lgd_iter = interim_path.joinpath("lgd_iter")
if not lgd_iter.exists():
    lgd_iter.mkdir(parents=True)
lgd_iter_file = lgd_iter.joinpath("lgd_iter_file.csv")


def lgd_master():
    def lgd_file_prep():
        # Importing LGD mapping index
        lgd = pd.read_csv(lgd_in_file, encoding="ISO8859")
        # lgd.drop("St_Cs2011_code", axis=1, inplace=True)

        # renaming the concerned columns
        lgd = lgd.rename(
            columns={
                "State Name (In English)": "state",
                "District Name  (In English)": "district",
                "Block Name (In English)": "block",
            }
        )

        lgd["state"] = lgd["state"].str.upper()
        lgd["district"] = lgd["district"].str.upper()
        lgd["block"] = lgd["block"].str.upper()

        # concatenating states, districts and blocks
        lgd["state_dist"] = lgd["state"] + lgd["district"]
        lgd["state_dist_block"] = lgd["state"] + lgd["district"] + lgd["block"]

        # print(lgd.columns)
        print("LGD file has been prepared")

        return lgd

    def data_file_prep():

        # importing the interim file
        df = pd.read_csv(in_file)

        # uppercasing the block names in state files
        df["block"] = df["block"].str.replace("_", " ").str.upper()
        df["state"] = df["state"].str.replace("_", " ").str.upper()
        df["district"] = df["district"].str.replace("_", " ").str.upper()

        print("Data file has been prepared")

        return df

    def block_name_clean():
        df = data_file_prep()
        conditions = [
            # Assam
            (df["state"] == "ASSAM")
            & (df["district"] == "BAJALI")
            & (df["block"] == "BHAWANIPUR"),
            # Himachal Pradesh
            (df["state"] == "HIMACHAL PRADESH")
            & (df["district"] == "MANDI")
            & (df["block"] == "DHARAMPUR MANDI"),
            (df["state"] == "HIMACHAL PRADESH")
            & (df["district"] == "SHIMLA")
            & (df["block"] == "TUTU"),
            # Karnataka
            (df["state"] == "KARNATAKA")
            & (df["district"] == "KALABURAGI")
            & (df["block"] == "GULBARGA"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU RURAL")
            & (df["block"] == "BANGALORE EAST"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU RURAL")
            & (df["block"] == "BANGALORE NORTH"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU RURAL")
            & (df["block"] == "BANGALORE SOUTH"),
            # Madhya Pradesh
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "NARMADAPURAM")
            & (df["block"] == "MAKHAN NAGAR"),
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "NARMADAPURAM")
            & (df["block"] == "NARMADAPURAM"),
            # Maharashtra
            (df["state"] == "MAHARASHTRA")
            & (df["district"] == "SANGLI")
            & (df["block"] == "WALWA"),
            # Meghalaya
            (df["state"] == "MEGHALAYA")
            & (df["district"] == "SOUTH WEST GARO HILLS")
            & (df["block"] == "DAMALGRE"),
            # Rajasthan
            (df["state"] == "RAJASTHAN")
            & (df["district"] == "NAGAUR")
            & (df["block"] == "KHEEWSAR"),
            # Tamil Nadu
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "THENI")
            & (df["block"] == "CUMBUM"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "ERODE")
            & (df["block"] == "T.N. PALAYAM"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "VILLUPURAM")
            & (df["block"] == "T.V. NALLUR"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "DINDIGUL")
            & (df["block"] == "BATLAGUNDU"),
        ]

        options = [
            # Assam
            "BHABANIPUR",
            # Himachal Pradesh
            "DHARMPUR",
            "MASHOBRA",
            # Karnataka
            "KALABURAGI",
            "BENGALURU EAST",
            "BENGALURU NORTH",
            "BENGALURU SOUTH",
            # Madhya Pradesh
            "BABAI",
            "HOSHANGABAD",
            # Maharashtra
            "VALVA-ISLAMPUR",
            # Meghalaya
            "RERAPARA",
            # Rajasthan
            "KHINVSAR",
            # Tamil Nadu
            "KAMBAM",
            "THOOCKANAICKENPALAIYAM",
            "THIRUVENNAINALLUR",
            "NILAKOTTAI",
        ]

        df["block"] = np.select(conditions, options, default=df["block"])

        return df

    def data_name_concater():
        df = block_name_clean()

        # concatenating states, districts and blocks for states

        df["state_dist"] = df["state"] + df["district"]
        df["state_dist_block"] = df["state"] + df["district"] + df["block"]

        return df

    def lgd_mapper():

        lgd = lgd_file_prep()
        df = data_name_concater()

        # lgd=lgd.iloc[:,[0,1,2,3,6]]  #to be applied to remove block name columns while iterating over districts
        lgd.drop_duplicates("state_dist_block", inplace=True)

        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        print(df1.columns)

        tabs = df1["_merge"].value_counts()

        print(tabs)

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            ["state_data", "district_data", "block_data", "state_dist_block"]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(
            subset="state_dist_block"
        )

        return [not_lgd_mapped, lgd, df]

    def fuzzy_mapper():

        output = lgd_mapper()
        not_lgd_mapped = output[0]
        lgd = output[1]
        df = output[2]

        # Fuzzywuzzy for mapping
        print("******* Initiating Fuzzy Mapping ********")

        # creating a list of matches for unmerged state_dist_block names
        result = [
            process.extractOne(i, lgd["state_dist_block"])
            for i in not_lgd_mapped["state_dist_block"]
        ]

        # converting and editing the fuzzy matches to a dataframe
        result = pd.DataFrame(result, columns=["match", "score", "id"])
        result.drop("id", axis=1, inplace=True)

        # creating a proxy dataframe with names of unmerged original names of state_dist_block
        not_lgd_proxy_df = (
            pd.DataFrame(not_lgd_mapped["state_dist_block"], index=None)
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
        df["state_dist_block"] = df["state_dist_block"].replace(mapper_dict)

        print(
            "*******Fuzzy Mapping for has ended. Proceeding for second round of Data Merge*******",
        )

        # Second round of data merge with update fuzzy matched names along with exact original names in the state file
        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        df1["_merge"].value_counts()

        # filtering fuzzy unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state_data",
                "district_data",
                "block_data",
                "state_dist_block",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(
            subset="state_dist_block"
        )

        tabs = df1["_merge"].value_counts()

        print(tabs)

        df1 = df1.rename(
            columns={
                # 'State Code':'state_lgd_code',
                "state_data": "state",
                # 'District Code':"district_lgd_code",
                "district_data": "district",
                " Block Code": "block_lgd_code",
                "block_LGD": "block",
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
            "gp",
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
        df1.to_csv(
            interim_path.joinpath("2022_23_June_block_lgd_mapped.csv"),
            index=False,
        )

        return df1

    fuzzy_mapper()

    return


lgd_master()
