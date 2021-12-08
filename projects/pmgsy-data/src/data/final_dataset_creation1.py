import pandas as pd
import numpy as np
from pathlib import Path
import glob

path=r'D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\interim\1_physical-progress-of-works'
file_list_final=[]

def get_filePath(local):
    """
    To get paths of all the files present in raw
    """
    for path in Path(local).iterdir():
        if path.is_file():
            # print(path)
            file_list_final.append(path)
            #Data_cleaning(path)
        else:
            get_filePath(path)
get_filePath(path)
dframe=[]
for file in file_list_final:
    df = pd.read_csv(file, index_col=None, header=0)
    dframe.append(df)
'''concating the dataframes'''
frame = pd.concat(dframe, axis=0, ignore_index=True)
frame.to_csv(r'D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\processed\final_dataset1.csv',index=False)