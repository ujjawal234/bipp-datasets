from pathlib import Path

dir_path = Path.cwd()

raw_path = dir_path.joinpath("data", "raw", "2021_22_March")

files = list(raw_path.glob("*/*/*/*.csv"))


for file in files:
    alpha = str(file).replace(" .csv", ".csv")
    # print(alpha)
    beta = alpha.replace(" ", "_")
    file = file.rename(beta)
    # print(beta)
