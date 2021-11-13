import pandas as pd


# Function for aggregrating Data
def aggregate(df, columns, values, filename):
    df2 = []
    for i in columns:
        variable1 = df.groupby(i)
        sums = variable1[values].sum()
        # print(sums)
        df2.append(sums)
    t = pd.DataFrame(df2)
    print(df2)
    t.to_csv(filename)


# Function for cleaning the data


def clean(file, column, value):
    group = file.groupby(column)
    grandsum = group[value].sum()
    maximum = group[value].max()
    diff = grandsum - maximum
    msum = maximum.sum()
    diffsum = diff.sum()
    print("Total Sum is: " + grandsum.sum())
    print("Maximum's Sum is: " + maximum.sum())
    print("Difference Sum is: " + diff.sum())
    if msum == diffsum:
        print("Data is being Cleaned.....")
        file.drop(maximum)
    else:
        print("No need to clean file......")


def subgroup_district(dataframe, districtname, socialgroup, unirrigatedarea):
    agg = dataframe.groupby([districtname, socialgroup], as_index=False).agg(
        {unirrigatedarea: "sum"}
    )
    print(agg)


def subgroup_tehsil(dataframe, tehsil, socialgroup, unirrigatedarea, irrigatedarea):
    agg = dataframe.groupby([tehsil, socialgroup], as_index=False).agg(
        {unirrigatedarea: "sum", irrigatedarea: "sum"}
    )
    print(agg)


def finddis(df, dis, name):
    query = df[df[dis] == name]
    print(query)


def aggregate_data(df):
    state = input("Please provide satate name from the state list\n")
    if state not in df["state_name"].values:
        print("Please enter correct value")
    else:
        state_df = df[df["state_name"] == state]
        all_states = (
            df.groupby(["state_name", "social_group"])
            .agg({"gca_unirr_ar_state": "sum"})
            .reset_index()
        )
        state = (
            state_df.groupby(["state_name", "social_group"])
            .agg({"gca_unirr_ar_state": "sum"})
            .reset_index()
        )

        district = (
            state_df.groupby(["district_name", "social_group"])
            .agg({"gca_unirr_ar_state": "sum"})
            .reset_index()
        )

        tehsil = (
            state_df.groupby(["tehsil_name", "social_group"])
            .agg({"gca_unirr_ar_state": "sum"})
            .reset_index()
        )

        all_tehsil_dist = (
            state_df.groupby(["district_name", "tehsil_name", "social_group"])
            .agg({"gca_unirr_ar_state": "sum"})
            .reset_index()
        )

        heirarchy = input(
            "Please input the level at which you want to group the data \n For state wise list enter 1 \n For District wise list enter 2 \nFor Tehsil wise list enter 3 \n"
        )

        if heirarchy == "1":
            dis_response = input(
                "For selected state data enter 1 \n For all states enter 2\n"
            )
            if dis_response == "1":
                print(state)
                value = state
            elif dis_response == "2":
                print(all_states)
                value = all_states

        elif heirarchy == "2":
            dis_response = input(
                """For a single district enter 1 \n
                   For all districts enter 2 \n"""
            )
            if dis_response == "1":
                print(state_df["district_name"].unique().sort())
                print("\n Please select any district from above given districts")
                dis_response2 = input("Enter District\n")

                if dis_response2 not in state_df["district_name"].values:
                    print("Enter correct district name, Try again!")
                else:
                    response = district[district["district_name"] == dis_response2]
                    print(response)
                    value = response

            elif dis_response == "2":
                print("Printing All districts data \n")
                print(district)
                value = district

        elif heirarchy == "3":
            dis_response = input(
                "For a single tehsil enter 1 \n For all tehsils grouped by districts enter 2\n"
            )
            if dis_response == "1":
                print(state_df["tehsil_name"].unique().sort())
                print("\n Please select any tehsil from above given districts")

                dis_response2 = input("Enter Tehsil\n")
                if dis_response2 not in state_df["tehsil_name"].values:
                    print("Enter correct tehsil namDelhe, Try again!")
                else:
                    response = tehsil[tehsil["tehsil_name"] == dis_response2]
                    print(response)
                    value = response

            elif dis_response == "2":
                print("Printing All districts data \n")
                print(all_tehsil_dist)
                value = all_tehsil_dist
        else:
            print("Try again with a valid response")
    filename = input("Please input Csv File name")
    csvname = "./data/processed/%s.csv" % (filename)
    value.to_csv(csvname)
