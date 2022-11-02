from pathlib import Path

import numpy as np
import pandas as pd
from fuzzywuzzy import process

# defining directories
dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = dir_path.joinpath("data", "external")

# change in lgd file name accordingly
lgd_in_file = external_path.joinpath("block_lgd.csv")

# change in file name accordingly
in_file = interim_path.joinpath("fl2_appended.csv")

# path to hold iterated files produced during LGD WIP
lgd_iter = interim_path.joinpath("lgd_iter")
if not lgd_iter.exists():
    lgd_iter.mkdir(parents=True)
lgd_iter_file = lgd_iter.joinpath("lgd_iter_file.csv")


def lgd_master():
    def lgd_file_prep():
        # Importing LGD mapping index
        lgd = pd.read_csv(lgd_in_file, encoding="ISO8859")
        lgd.drop("St_Cs2011_code", axis=1, inplace=True)

        # renaming the concerned columns
        lgd = lgd.rename(
            columns={
                "State Name(In English)": "state",
                "District Name(In English)": "district",
                "Block Name (In English) ": "block_name",
            }
        )

        lgd["state"] = lgd["state"].str.upper()
        lgd["district"] = lgd["district"].str.upper()

        # concatenating states, districts and blocks
        lgd["state_dist"] = lgd["state"] + lgd["district"]

        # print(lgd.columns)
        print("LGD file has been prepared")

        return lgd

    def data_file_prep():

        # importing the interim file
        df = pd.read_csv(in_file)

        # uppercasing the block names in state files
        df["state"] = df["state"].str.replace("_", " ").str.upper()
        df["district"] = df["district"].str.replace("_", " ").str.upper()

        print("Data file has been prepared")

        return df

    def dist_name_clean():
        df = data_file_prep()

        conditions = [
            # Andhra Pradesh
            (df["state"] == "ANDHRA PRADESH") & (df["district"] == "CUDDAPAH"),
            (df["state"] == "ANDHRA PRADESH") & (df["district"] == "NELLORE"),
            # Assam
            (df["state"] == "ASSAM")
            & (df["district"] == "DIMA HASAO NORTH CACHAR HILLS"),
            (df["state"] == "ASSAM") & (df["district"] == "KAMRUPMETRO"),
            (df["state"] == "ASSAM") & (df["district"] == "MORIGAON"),
            (df["state"] == "ASSAM")
            & (df["district"] == "SOUTH SALMARAMANKACHAR"),
            # Bihar
            (df["state"] == "BIHAR") & (df["district"] == "AURANAGABAD"),
            # Chattisgarh
            (df["state"] == "CHHATTISGARH") & (df["district"] == "KAWARDHA"),
            # Madhya Pradesh
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "NARMADAPURAM"),
            # Sikkim
            (df["state"] == "SIKKIM") & (df["district"] == "EAST DISTRICT"),
            (df["state"] == "SIKKIM") & (df["district"] == "NORTH DISTRICT"),
            (df["state"] == "SIKKIM") & (df["district"] == "SOUTH DISTRICT"),
            (df["state"] == "SIKKIM") & (df["district"] == "WEST DISTRICT"),
            # Tamil Nadu
            (df["state"] == "TAMIL NADU") & (df["district"] == "THOOTHUKKUDI"),
            # Telangana
            (df["state"] == "TELANGANA") & (df["district"] == "BADRADRI"),
            (df["state"] == "TELANGANA") & (df["district"] == "KOMARAM BHEEM"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "YADADRI BHONGIR"),
            # Uttar Pradesh
            (df["state"] == "UTTAR PRADESH")
            & (df["district"] == "SANT RAVIDAS NAGAR"),
            # West Bengal
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "PASCHIM MEDINIPUR"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "PURBA MEDINIPUR"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "SILIGURI MAHAKUMA PARISHAD DMMU"),
            # Punjab
            (df["state"] == "PUNJAB") & (df["district"] == "MUKTSAR"),
        ]

        options = [
            "Y.S.R.",
            "SPSR NELLORE",
            # Assam
            "DIMA HASAO",
            "KAMRUP METRO",
            "MARIGAON",
            "SOUTH SALMARA MANCACHAR",
            # Bihar
            "AURANGABAD",
            # Chattisgarh
            "KABIRDHAM",
            # Madhya Pradesh
            "HOSHANGABAD",
            # Sikkim
            "GANGTOK",
            "MANGAN",
            "NAMCHI",
            "GYALSHING",
            # Tamil Nadu
            "TUTICORIN",
            # Telangana
            "BHADRADRI KOTHAGUDEM",
            "KUMURAM BHEEM ASIFABAD",
            "YADADRI BHUVANAGIRI",
            # Uttar Pradesh
            "BHADOHI",
            # West Bengal
            "MEDINIPUR WEST",
            "MEDINIPUR EAST",
            "DARJEELING",
            # Punjab
            "SRI MUKTSAR SAHIB",
        ]

        df["district"] = np.select(conditions, options, default=df["district"])

        return df

    def data_name_concater():
        df = dist_name_clean()

        # concatenating states, districts and blocks for states

        df["state_dist"] = df["state"] + df["district"]

        return df

    def lgd_mapper():

        lgd = lgd_file_prep()
        df = data_name_concater()

        # lgd=lgd.iloc[:,[0,1,2,3,6]]  #to be applied to remove block name columns while iterating over districts
        lgd.drop_duplicates("state_dist", inplace=True)

        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist",
            right_on="state_dist",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        print(df1.columns)

        tabs = df1["_merge"].value_counts()

        print(tabs)

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            ["state_data", "district_data", "state_dist"]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

        return [not_lgd_mapped, lgd, df]

    def fuzzy_mapper():

        output = lgd_mapper()
        not_lgd_mapped = output[0]
        lgd = output[1]
        df = output[2]

        # Fuzzywuzzy for mapping
        print("******* Initiating Fuzzy Mapping ********")

        # creating a list of matches for unmerged state_dist names
        result = [
            process.extractOne(i, lgd["state_dist"])
            for i in not_lgd_mapped["state_dist"]
        ]

        # converting and editing the fuzzy matches to a dataframe
        result = pd.DataFrame(result, columns=["match", "score", "id"])
        result.drop("id", axis=1, inplace=True)

        # creating a proxy dataframe with names of unmerged original names of state_dist
        not_lgd_proxy_df = (
            pd.DataFrame(not_lgd_mapped["state_dist"], index=None)
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
        df["state_dist"] = df["state_dist"].replace(mapper_dict)

        print(
            "*******Fuzzy Mapping for has ended. Proceeding for second round of Data Merge*******",
        )

        # Second round of data merge with update fuzzy matched names along with exact original names in the state file
        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist",
            right_on="state_dist",
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
                "state_dist",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(subset="state_dist")

        tabs = df1["_merge"].value_counts()

        print(tabs)

        df1 = df1.rename(
            columns={
                "St_LGD_code": "state_lgd_code",
                "state_LGD": "state",
                "Dt_LGD_code": "district_lgd_code",
                "district_LGD": "district",
            }
        )

        final_col_list = [
            "year",
            "month",
            "state_lgd_code",
            "state",
            "district_lgd_code",
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

        df1 = df1[final_col_list]

        # print("Exporting not_lgd and LGD matched as CSV file to the Interim sub directory")
        not_lgd_mapped.to_csv(lgd_iter_file, index=False)
        lgd.to_csv(str(lgd_iter) + "/lgd.csv", index=False)
        df1.to_csv(
            interim_path.joinpath("fl2_district_lgd_mapped.csv"), index=False
        )

        return df1

    fuzzy_mapper()

    return


lgd_master()
