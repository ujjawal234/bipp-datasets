# import modules
import pandas as pd
import os
import csv

#method to read files from a folder and return combined dataframe
def read_folder(csv_folder):  
  files = os.listdir(csv_folder)
  df_list = [] # create empty list
  csv.field_size_limit(100000000)
  # append datasets to the list
  for f in files:
    csv_file = csv_folder + "/" + f
    df_list.append(pd.read_csv(csv_file,engine='python', sep=',', quotechar='"', error_bad_lines=False))
  for d in df_list:
        d.replace(r"^ +| +$", r"", regex=True, inplace=True)   #to remove trailing and leading spaces in each dataframe
  df_full = pd.concat(df_list, ignore_index=True)  # to concatenate dataframes in one dataframe
  return df_full

#function call
combined_dataframe = read_folder(r"bipp-datasets\projects\fertilizer-mis\data\raw\2_dealer_data")

#to print dimensions of dataframe
print(combined_dataframe.shape)

#saving dataframe as csv
combined_dataframe.to_csv(r'bipp-datasets\projects\fertilizer-mis\data\interim\dealer_data.csv',index=False)

