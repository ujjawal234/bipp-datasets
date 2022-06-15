# import glob
import os

# import shutil
# from ntpath import join
from pathlib import Path

import pandas as pd

# from numpy import nan

# Getting the directories of the data folders
project_folder = str(Path(__file__).resolve().parents[2])
raw_folder = project_folder + r"/data/raw/2_physical-and-financial-project-summary"
interim_folder = project_folder + r"/data/interim/"


# defining empty file lists
file_list = []
modified_file_list = []


def get_filePath(local):
    """
    To get paths of all the files present in raw
    """
    for path in Path(local).iterdir():
        if path.is_file():
            # print(path)
            file_list.append(path)
            # Data_cleaning(path)
        else:
            get_filePath(path)


def filteringpathnames(list):
    """
    To remove the files if there is All Collaborations or All Batches in the file names
    """
    for path in list:
        stri = str(path)
        if stri.find("All Collaborations") == -1:
            if stri.find("All Batches") == -1:
                modified_file_list.append(path)
                pass


def datacleaning(file_path):
    """
    Gets the State, district and tehsil names from the path, reads the file and does minor data operations on each file
    """
    path_string = str(file_path)
    path_str_list = path_string.split("\\")
    print(path_str_list)
    if len(path_str_list) == 13:
        # tehsil = path_str_list[-3]
        # district = path_str_list[-4].replace("+", " ")
        state_list = path_str_list[-5].replace("+", " ")
    else:
        # tehsil = path_str_list[-4]
        # district = path_str_list[-5].replace("+", " ")
        state_list = path_str_list[-6].replace("+", " ")
    dest_path = path_string.replace("raw", "interim")
    dest_path_list = dest_path.split("\\")
    dest_path = ("\\").join(dest_path_list[:-1])
    dest_file_name = dest_path_list[-1].replace(" ", "").lower()
    if not os.path.isdir(dest_path):
        os.makedirs(dest_path)
    raw_df = pd.read_csv(file_path)
    cleaned_df = raw_df.iloc[1:, 2:]
    cleaned_df.columns = cleaned_df.iloc[0]
    cleaned_df.reset_index(drop=True, inplace=True)
    cleaned_df["state_name"] = state_list
    cleaned_df = cleaned_df.replace("-", "")
    cols = ["Road Length", "Total Population"]
    for col in cols:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors="coerce")
        cleaned_df[col].fillna(0)
    cleaned_df.columns = cleaned_df.columns.str.lower()
    print
    cleaned_df.drop([0], inplace=True)
    cleaned_df.drop([len(cleaned_df)], inplace=True)  # dropping row with total
    cleaned_df = cleaned_df.iloc[:, [0, 14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]]
    # renamed=state+'-'+district+'-'+tehsil+'-'+k[-1]+'.csv'
    file_name = dest_path + "/" + dest_file_name
    cleaned_df.to_csv(file_name, index=False)


# Calling the functions

# 1. to get all the paths

get_filePath(raw_folder)

# 2. remove unnecessary files from the file paths.

filteringpathnames(file_list)

# 3. iterating through each path to clean the file

for file in modified_file_list:
    print(file)
    datacleaning(file)
