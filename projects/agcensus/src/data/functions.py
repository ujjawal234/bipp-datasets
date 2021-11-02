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
