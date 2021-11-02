import functions as f
import pandas as pd

df = pd.read_csv("./data/raw/Cleaned.csv")

selectable = []
selectable2 = []
print(
    "Please Select Column for grouping the data"
    + "\n"
    + "The Selection must be in numbers stating from 0 till the desired columns"
)
col = df.select_dtypes(["object"]).columns
col2 = df.select_dtypes(["float64"]).columns

for i in col:
    selectable.append(i)
print(selectable)
response1 = int(input("Insert choice \n"))

for i in range(response1):
    k = i + 1
    getvar = col[:k]

print(
    "Please Select Variables which are needed to be grouped"
    + "\n"
    + "The Selection must be in numbers stating from 0 till the desired variable"
)
for i in col2:
    selectable2.append(i)

response2 = int(input("Insert choice \n"))

for j in range(response2):
    getvar2 = col2[j]
print(getvar2)

fname = input("Please provide csv file name \n")
filename = fname + ".csv"
result = f.aggregate(df, getvar, getvar2, filename)
