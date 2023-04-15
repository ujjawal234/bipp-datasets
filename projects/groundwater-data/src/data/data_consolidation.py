# %%
# import libraries
from pathlib import Path

import pandas as pd

# %%
# defining paths to the folders
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = project_dir + "/data/raw/"
imd_raw_folder = project_dir + "/data/raw/"
imd_raw_folder_path = Path(imd_raw_folder)
imd_raw_folder_path.mkdir(parents=True, exist_ok=True)

# %%


def pathfinder(directory: str, file_type: str):
    """
    A function retrive file paths for a specific file type in a directory.

    params:

    directory - path to the folder from which we need the files
    file_type - file extension. Ex: csv or pdf.

    Note please do not add "." before the value of file type. value should be "csv" not ".csv"
    """
    # Create a Path object for the directory
    dir_path = Path(directory)

    # Get all the files in the directory
    files = dir_path.glob(f"**/*.{file_type}")

    # Create a list of file names
    file_list = [file.joinpath() for file in files]

    # Return the list of file names
    return file_list


# %%
file_list = pathfinder(directory=imd_raw_folder + "/state_level", file_type="csv")
# %%

# file consolidator


def consolidator(file_list: list):
    """
    Function to consolidate the list of all the files in the file_list

    Params:

    file_list
    """
    # Read Data
    temp_list = []
    # iterating through the files
    for file in file_list:
        data = pd.read_csv(file)
        print(data.head())
        temp_list.append(data)

    dataframe = pd.concat(temp_list).reset_index(drop=True)

    return dataframe


dataset = consolidator(file_list=file_list)
# %%
# dataset cleaning and Transformation Functions


def transformator(dataset: pd.DataFrame, level: str):
    """
    function to clean and transform state level, district level and station level data for ground water daily datasets

    Parms:

    dataset: A DataFrame which has all the consolidated data
    level: state or district or station level as string values
    """
    transformed_dataset = dataset
    file_name = level

    return transformed_dataset, file_name
