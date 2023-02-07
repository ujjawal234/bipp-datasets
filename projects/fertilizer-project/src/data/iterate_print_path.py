from pathlib import Path
import pandas as pd
directory = r"D:\Sambhav\bipp-datasets\projects\fertilizer-mis-1\data\raw"

# a list to store the addresses of the csv files
list = []

# function to check if the current csv file has all pages from 1 to highest number
def not_missing(path):
    df = pd.read_csv(path) # to read the csv file and create our dataframe
    maximum = 0 # to store the highest page number
    s = set() # a set to store all the unique page number values
    # traversing the page column and storing the page numbers into the set and finding the highest value
    for data in df["Page"]: 
        s.add(data)
        maximum = max(maximum, data)

    return len(s) == maximum # checks whether the size of the set is equal to the maximum page value


# function to traverse the csv files and store the address of the required csv files
def printpath(directory):

    # loop to iterate through all the csv files and check if it has missing page numbers
    for path in Path(directory).iterdir():  
        if path.is_file():
            path = str(path)
            dir_path = path[len(path)-4:len(path)]
            if dir_path == ".csv":
                if not not_missing(path): # if the csv file has missing page numbers then we insert it into the list
                    print(path)
                    list.append(path)
                
        else:
            printpath(path)

# calling printpath function
printpath(directory) 
# printing all the required csv files
for path in list: 
    print(path) 