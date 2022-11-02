from pathlib import Path

import pandas as pd

# defining directories
dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = dir_path.joinpath("data", "external")

# change in lgd file name accordingly
lgd_in_file = external_path.joinpath("all_pri.xlsx")
out_file = external_path.joinpath("all_pri.csv")

df = pd.read_excel(
    lgd_in_file, header=3, sheet_name="Report", engine="openpyxl"
)
# print(len(col_names))

df = df.iloc[1:, [1, 2, 7, 9]]
print(df)

col_names = [x.strip().replace(" ", "_").lower() for x in df.columns]
df.columns = col_names

print(col_names)

data_list = []

data_list.append(df)

for i in range(1, 5, 1):
    print(f"Reading sheet {i}")
    df1 = pd.read_excel(
        lgd_in_file,
        header=None,
        sheet_name="Report" + str(i),
        engine="openpyxl",
    )
    # print(df1.columns)
    df1 = df1.iloc[:, [1, 2, 7, 9]]
    df1.columns = col_names
    print(df1)

    data_list.append(df1)

final_data = pd.concat(data_list)

print(final_data["state_name"].unique())

final_data.to_csv(out_file, index=False)
print("Final file concated and exported.")
