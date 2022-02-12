# import liberaries
import pandas as pd
import os

# dataframe of final output file
dataframe= []

# recursively iterate over all the csv files stored in a row folder
def iterate(path):
    if os.path.isfile(path):
        if path.split('.')[-1]=='csv':
            df = pd.read_csv(path)
            global dataframe
            if len(dataframe) == 0:
                dataframe = df
            else:
                dataframe = pd.concat([dataframe, df], ignore_index=True)
        return
    for file in os.listdir(path):
        iterate(os.path.join(path,file))

# saving the dataframe
iterate('data/raw')

# save the consoladated csv file into interim folder
dataframe.to_csv("data/interim/output.csv", index=False)