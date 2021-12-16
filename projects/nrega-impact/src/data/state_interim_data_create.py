import pathlib

import numpy as np
import pandas as pd

finish_points = []

# Function to call each csv file and spit out clean csv


def nrega_data_appender(path_name):

    files = list(pathlib.Path(path_name).glob("data/raw/NREGA_assets_raw/*.csv"))

    for file in files:
        # Importing TN data
        TN = pd.read_csv(file, encoding="ISO8859")
        print(file.stem, " has been loaded.")

        # removing works with deleted work_status
        def work_status_filter(data):
            data = data[data["work_status"].isin(["Completed", "Physically Completed"])]
            data = data.drop("work_name", axis=1)
            return data

        TN = work_status_filter(TN)

        # special state filters
        def special_rows_filter(data):
            if any(data["state"] == "KERALA"):
                data = data[(data["block_name"] != "KURWAI")]
            elif any(data["state"] == "TELANGANA"):
                data = data[(data["panchayat_name"].notna())]

            return data

        TN = special_rows_filter(TN)

        # stripping trailing lines in strings
        def trail_strip(data):
            for i in data.select_dtypes(include="object").columns.tolist():
                if any(data[i].str.contains("01-01-1900", regex=False)):
                    data[i] = data[i].str.replace("01-01-1900", "")
                data[i] = data[i].str.rstrip()
            return data

        TN = trail_strip(TN)

        # removing digits and special characters from name strings
        def nrega_string_clean(data):
            if file.stem != "RAJASTHAN":
                for i in ["panchayat_name", "block_name", "district", "state"]:
                    meta_char = r"\#|\&|\@|\$|\%|\^|\*|\(|\)|\)|\_|\+|\=|\\|\/|\?|\>|\<|\:|\;|\`|\~|\!"
                    if any(data[i].str.contains(r"\d+", regex=True)):
                        data[i] = data[i].str.replace(r"\d+", "")
                    if any(data[i].str.contains(meta_char)):
                        data[i] = data[i].str.replace(meta_char, "")

                for i in [
                    "master_work_category_name",
                    "work_category_name",
                    "work_type",
                    "agency_name",
                ]:
                    if any(data[i].str.contains(r"^<", regex=True)):
                        data[i] = data[i].replace(r"<.*", np.nan, regex=True)

            else:
                # importing devanagiri script to make unicode dictionary

                print(
                    "*********************Handling Dictionaries for",
                    file.stem,
                    "***************************",
                )
                devan = pd.read_csv(".\data\external\devanagiri.csv")

                # creating unicode dictionary
                uni_dict = dict(zip(devan["1_x"], devan["0"]))

                # creating english dictionary
                eng_dict = dict(zip(devan["0"], devan["1_y"]))

                # mapping the unicode and english dictionary to the panchayat column of Rajasthan
                print("Converting the strings of Panchayat Name in Rajasthan")
                data["panchayat_name"] = data["panchayat_name"].str.replace("<U\+", "")
                data["panchayat_name"] = data["panchayat_name"].str.split(r"\>")
                data["panchayat_name"] = data["panchayat_name"].apply(
                    lambda x: list(filter(None, x))
                )
                data["panchayat_name"] = data["panchayat_name"].map(
                    lambda x: [uni_dict.get(k) for k in x]
                )
                data["panchayat_name"] = data["panchayat_name"].map(
                    lambda x: [eng_dict.get(k) for k in x]
                )
                data["panchayat_name"] = data["panchayat_name"].apply(
                    lambda x: list(filter(None, x))
                )
                data["panchayat_name"] = data["panchayat_name"].str.join(sep=",")
                data["panchayat_name"] = data["panchayat_name"].str.replace(",", "")
                data["panchayat_name"] = data["panchayat_name"].str.upper()

                print("Rajasthan's Panchayat names have been cleaned")
                print(
                    "*************************************************************************"
                )

            return data

        TN = nrega_string_clean(TN)

        # rectifying date coumn types
        def date_cleaner(data):
            for i in ["work_started_date", "work_physically_completed_date"]:
                if data[i].dtype == "O":
                    if any(data[i].str.contains(r"[A-Z]|[a-z]", regex=True)):
                        data[i] = data[i].str.replace(r"[A-Z]|[a-z]", "")
                    data[i] = pd.to_datetime(data[i], errors="coerce")
                else:
                    data[i] = pd.to_datetime(data[i], errors="coerce")
            # data[i]=pd.to_datetime(data[i], errors='coerce')
            return data

        TN = date_cleaner(TN)

        # identifying points where 'work_started_date'>'work_physically_completed_date'
        def early_complete(data):
            conditions = [
                data["work_started_date"] > data["work_physically_completed_date"],
                data["work_started_date"] == data["work_physically_completed_date"],
                data["work_started_date"] < data["work_physically_completed_date"],
                data["work_physically_completed_date"].isna(),
                data["work_started_date"].isna(),
            ]
            options = [
                "Finished before start",
                "On the same day",
                "After start",
                "End date missing",
                "Start date missing",
            ]
            data["finished_when"] = np.select(conditions, options)
            return data

        TN = early_complete(TN)

        # ISID: Attempting to identify each row uniquely by a combination of column values
        def isid(
            data, col_names
        ):  # could avoid the col_names and give these cols as default
            data = data.set_index(col_names)
            if data.index.is_unique:
                data = data.reset_index()
                print(
                    "The selected columns of ",
                    TN["state"].unique(),
                    "make a unique key",
                )
            else:
                print(
                    "The selected columns of ",
                    TN["state"].unique(),
                    "doesn't make a unique key",
                )
                print("Checking for missing values in the coulmns")
                data = data.reset_index()
                for (
                    i
                ) in (
                    col_names
                ):  # checking presence of missing values in the concerend columns
                    if data[i].isna().value_counts()[False] == len(data[i]):
                        print(str("No missing values in " + i))
                    else:
                        print(
                            str("Missing values found in " + i)
                        )  # didnt find any missing vals until now. If present add fucntion to deal with it
                print(
                    "Checking for duplicates....."
                )  # checking presence of duplicate values in the concerend columns
                data["dups"] = data.groupby(
                    col_names
                ).cumcount()  # creating duplicate identifier 'dups' to using grouped subsets of rows
                print(data["dups"].value_counts())  # Taking count of dups
                print(
                    "The following are a snapshot of duplicate values, at top 25 rows"
                )
                cols_names = col_names + [
                    "dups"
                ]  # Creating a second duplicate identifier so that dupicates can be displayed
                data["dups"] = data.groupby(col_names).cumcount()
                data["dups2"] = data.groupby(col_names)["dups"].transform(max)
                print(
                    data.loc[(data["dups2"] > 0), cols_names]
                    .sort_values(cols_names)
                    .head(25)
                )
                print(
                    "The following are a snapshot of duplicate values, at bottom 25 rows"
                )
                print(
                    data.loc[(data["dups2"] > 0), cols_names]
                    .sort_values(cols_names)
                    .tail(25)
                )
                print("Removing the duplicates")
                duplicates = data[data["dups2"] > 0].sort_values(
                    col_names
                )  # extcacts the duplicate entires to another folder for each state
                duplicates.to_csv(
                    str("data/interim/duplicate_entries/") + file.name, index=False
                )
                print(duplicates)
                data = data[(data["dups"] == 0)]
                data.drop(["dups", "dups2"], axis=1)
            return data

        TN = isid(
            TN, ["block_name", "panchayat_name", "work_code", "work_started_date"]
        )

        # sending info on early_complete to an external list
        def finish_data_maker(data):
            finish_date_temp_data = pd.DataFrame(data["finished_when"].value_counts())
            finish_date_temp_data = finish_date_temp_data.rename(
                columns={"finished_when": file.stem}
            )
            finish_points.append(finish_date_temp_data)

        finish_data_maker(TN)

        # rearranging columns
        def col_rearrange(data):
            cols_new = [
                "s_no",
                "state",
                "district",
                "block_name",
                "panchayat_name",
                "work_code",
                "work_started_date",
                "work_physically_completed_date",
                "finished_when",
                "sanction_amount_in_lakh",
                "total_amount_paid_since_inception_in_lakh",
                "total_mandays",
                "no_of_units",
                "is_secure",
                "work_status",
                "master_work_category_name",
                "work_category_name",
                "work_type",
                "agency_name",
                "work_start_fin_year",
            ]  # to put s.no as the first column. Happened after index reset in ISID
            data = data[cols_new]
            return data

        TN = col_rearrange(TN)

        # sending the cleaned file to interim folder
        print("Preview of", file.stem, "before CSV export:")
        print(TN.head(25))
        TN.to_csv(str("data/interim/NREGA_assets/") + file.name, index=False)
        print(file.stem, "has been exported as CSV file to interim data directory")

    # cleaning and exporting info from finish_data_maker to interim folder
    finish_file = pd.concat(finish_points, axis=1).reset_index()
    finish_file = finish_file.rename(columns={"index": "label"})
    finish_file = finish_file.melt(
        id_vars=["label"], value_name="vals", var_name="State"
    )
    finish_file = finish_file.pivot(
        index="State", columns="label", values="vals"
    ).reset_index()
    finish_file["Total observations"] = finish_file.sum(axis=1)
    print(
        "The following table is the comparison of start date and end date of works for each state"
    )
    print(finish_file)
    finish_file.to_csv("data/interim/finish_date.csv", index=False)
    print(
        "The comparison of start date and end date has been exported as excel file to the interim directory"
    )
    print("Looping has ended. Check the directory data/interim to find outputs")


nrega_data_appender(".")
