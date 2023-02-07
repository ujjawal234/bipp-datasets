import pandas as pd

df = pd.read_csv("./data/raw/cleaned.csv")
print("Available states for Analysis \n")
print(df["state_name"].unique())

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
        "Please input the level at which you want to group the data \n For state wise list enter 1 \n For District wise list enter 2 \n For Tehsil wise list enter 3 \n"
    )
    if heirarchy == "1":
        dis_response = input(
            """For selected state data enter 1 \n For all states enter 2\n"""
        )
        if dis_response == "1":
            print(state)
            value = state
        elif dis_response == "2":
            print(all_states)
            value = all_states
    elif heirarchy == "2":
        dis_response = input(
            "For a single district enter 1 \n For all districts enter 2 \n"
        )
        if dis_response == "1":
            print(state_df["district_name"].unique().sort())
            print("\n Please select any district from above given districts\n")
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
