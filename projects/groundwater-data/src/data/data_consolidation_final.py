# %%
# import libraries
import json
import os
from pathlib import Path

import pandas as pd

# defining paths to the folders
project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = os.path.join(project_dir, "data", "raw")
ext_folder = os.path.join(project_dir, "data", "external")
interim_folder = os.path.join(project_dir, "data", "interim")
imd_raw_folder = os.path.join(project_dir, "data", "raw")
processed = os.path.join(project_dir, "data", "processed")
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


state_file_list = pathfinder(directory=imd_raw_folder + "/state_level", file_type="csv")
district_file_list = pathfinder(
    directory=imd_raw_folder + "/district_level", file_type="csv"
)

# LGD Mapper Functions

# %%


def get_state_lgd_code(state_name: str):
    """
    Function to map LGD Code at the state level by reading the state_lgd_code file in external folder

    params:
    state_name: name of the state
    """
    state_name = str(state_name).upper()
    # read the JSON data from the file
    with open(ext_folder + "State_LGD_Code.json", "r") as infile:
        data = json.load(infile)
    # check if the state name is in the dictionary
    if state_name in data:
        # return the LGD Code for the state
        return str(data[state_name]["LGD Code"])
    else:
        # if the state name is not in the dictionary, return None
        return None


def get_district_lgd_code(district_name: str):
    """
    Function to map LGD Code at the state level by reading the state_lgd_code file in external folder

    params:
    district_name: name of the district
    """
    district_name = str(district_name).upper()
    # read the JSON data from the file
    with open(ext_folder + "District_LGD_Code.json", "r") as infile:

        data = json.load(infile)
    # check if the state name is in the dictionary
    if district_name in data:
        # return the LGD Code for the state
        return str(data[district_name]["District LGD Code"])
    else:
        # if the state name is not in the dictionary, return None
        return None


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


# %%
state_dataset = consolidator(file_list=state_file_list)
district_dataset = consolidator(file_list=district_file_list)

# dataset cleaning and Transformation Functions

# %%


def state_level_transformer(dataset: pd.DataFrame):
    """
    function to clean and transform state level, district level and station level data for ground water daily datasets and add LGD Codes respectively

    Parms:

    dataset: A DataFrame which has all the consolidated data
    level: state_level or district_level or station_level as string values
    """
    # removing columns with only null values
    dataframe = dataset.dropna(how="all", axis=1)
    # if level == "state_level":
    # drop the unnecessary columns
    dataframe = dataframe.drop(columns=["uuid", "egt", "dex"])
    # re-order the columns
    dataframe = dataframe[
        ["date", "state_name", "nst", "currentLevel", "noOfStations", "gwstorage"]
    ]
    # changing the date type
    dataframe["date"] = pd.to_datetime(dataframe["date"], format="%d-%m-%Y")
    # transforming the state_name column
    dataframe["state_name"] = dataframe["state_name"].str.title()
    dataframe["state_name"] = dataframe["state_name"].astype("category")
    dataframe = dataframe.replace("Daman & Diu", "Daman and Diu")
    # correcting spelling mistakes of state_name
    dataframe = dataframe.replace("Andaman & Nicobar", "Andaman and Nicobar Islands")
    dataframe = dataframe.replace("Dadra And Nagar Hav", "Dadra And Nagar Haveli")
    dataframe = dataframe.replace("Jammu & Kashmir", "Jammu and Kashmir")
    dataframe = dataframe.sort_values(by="date", inplace=False)
    # removing "total" values in the data
    dataframe = dataframe[dataframe["state_name"] != "Total"]
    # creating a state_code column with blank values
    dataframe["state_code"] = ""
    data_list = []
    # Iterating through each row to get the state_code
    for each_state in dataframe["state_name"].unique():
        filtered_data = dataframe[dataframe["state_name"] == each_state]
        filtered_data["state_code"] = get_state_lgd_code(state_name=each_state)
        data_list.append(filtered_data)

    dataframe = pd.concat(data_list)
    dataframe = dataframe[
        [
            "date",
            "state_name",
            "state_code",
            "nst",
            "currentLevel",
            "noOfStations",
            "gwstorage",
        ]
    ]
    dataframe.columns = [
        "date",
        "state_name",
        "state_code",
        "total_stations",
        "current_level",
        "no_stations_central",
        "gw_storage",
    ]
    file_name = "state_level_groundwater.csv"

    return dataframe, file_name


# %%
def district_level_transformer(dataset: pd.DataFrame):
    """
    function to clean and transform state level, district level and station level data for ground water daily datasets and add LGD Codes respectively

    Parms:

    dataset: A DataFrame which has all the consolidated data
    level: state_level or district_level or station_level as string values
    """
    # remove columns with full null values
    dataframe = dataset.dropna(how="all", axis=1)
    # drop the unnecessary columns
    dataframe = dataframe.drop(columns=["uuid", "egt", "dex"])
    # re-order the columns
    dataframe = dataframe[
        [
            "date",
            "state_name",
            "district_name",
            "nst",
            "currentLevel",
            "noOfStations",
            "gwstorage",
        ]
    ]
    # changing the date type
    dataframe["date"] = pd.to_datetime(dataframe["date"], format="%d-%m-%Y")
    # transforming the state_name column
    dataframe["state_name"] = dataframe["state_name"].str.title()
    print(dataframe["state_name"].unique())
    dataframe["state_name"] = dataframe["state_name"].astype("category")
    dataframe["district_name"] = dataframe["district_name"].str.title()
    dataframe["district_name"] = dataframe["district_name"].str.strip()
    dataframe["district_name"] = dataframe["district_name"].astype("category")
    dataframe["district_name"] = dataframe["district_name"].str.replace("&", "and")
    dataframe = dataframe.replace("Daman & Diu", "Daman and Diu")
    # correcting spelling mistakes of state_name
    dataframe = dataframe.replace("Andaman & Nicobar", "Andaman and Nicobar Islands")
    dataframe = dataframe.replace("Dadra And Nagar Hav", "Dadra And Nagar Haveli")
    dataframe = dataframe.replace("Jammu & Kashmir", "Jammu and Kashmir")
    dataframe = dataframe.replace("Lakshdweep", "Lakshadweep")
    dataframe = dataframe.replace("Pondicherry", "Puducherry")

    mapping = {
        "Garhwal": "Pauri Garhwal",
        "Viluppuram": "Villupuram",
        "Virudunagar": "Virudhunagar",
        "Gurgaon": "Gurugram",
        "Nagappattinam": "Nagapattinam",
        "Thoothukkudi": "Tuticorin",
        "Bauda": "Boudh",
        "Jhunjhunun": "Jhunjhunu",
        "Hardwar": "Haridwar",
        "Kodarma": "Koderma",
        "Dakshin Dinajpur": "Dinajpur Dakshin",
        "Darjiling": "Darjeeling",
        "Puruliya": "Purulia",
        "Hugli": "Hooghly",
        "Purba Medinipur": "Medinipur East",
        "Uttar Dinajpur": "Dinajpur Uttar",
        "Pashchim Medinipur": "Medinipur West",
        "Haora": "Howrah",
        "South 24 Parganas": "24 Paraganas South",
        "North 24 Parganas": "24 Paraganas North",
        "Koch Bihar": "Coochbehar",
        "Buldana": "Buldhana",
        "Bellary": "Ballari",
        "Mysore": "Mysuru",
        "Chikmagalur": "Chikkamagaluru",
        "Chamrajnagar": "Chamarajanagara",
        "Shimoga": "Shivamogga",
        "Tumkur": "Tumakuru",
        "Belgaum": "Belagavi",
        "Jyotiba Phule Nagar": "Amroha",
        "Bara Banki": "Barabanki",
        "Bangalore Rural": "Bengaluru Rural",
        "Sant Kabir Nagar": "Sant Kabeer Nagar",
        "Sant Ravi Das Nagar(Bhadohi)": "Bhadohi",
        "Mewat": "Nuh",
        "Narsimhapur": "Narsinghpur",
        "Jalor": "Jalore",
        "Bagalkot": "Bagalkote",
        "Debagarh": "Deogarh",
        "Kancheepuram": "Kanchipuram",
        "Lakshadweep": "Lakshadweep District",
        "Ladakh Dist1": "Leh Ladakh",
        "Pashchimi Singhbhum": "West Singhbhum",
        "Purbi Singhbhum": "East Singhbum",
        "Uttarkashi": "Uttar Kashi",
        "The Dangs": "Dang",
        "Dhaulpur": "Dholpur",
        "Purba Champaran": "Purbi Champaran",
        "Hoshangabad": "Narmadapuram",
        "Garhchiroli": "Gadchiroli",
        "Rangareddy": "Ranga Reddy",
        "Subarnapur": "Sonepur",
        "Nicobar": "Nicobars",
        "Saran (Chhapra)": "Saran",
        "Sri Potti Sriramulu Nellore": "SPSR Nellore",
        "Kabeerdham": "Kabirdham",
        "Leh (Ladakh)": "Leh Ladakh",
        "Saraikela-Kharsawan": "Saraikela Kharsawan",
        "Rudraprayag": "Rudra Prayag",
        "Davanagere": "Davangere",
        "Shrawasti": "Shravasti",
        "Sahibzada Ajit Singh Nagar": "S.A.S NAGAR",
        "Firozpur": "Ferozepur",
        "Kansiram Nagar": "Kasganj",
        "Bangalore": "Bengaluru Urban",
        "Koriya": "Korea",
        "Ahmadnagar": "Ahmednagar",
        "Sahibganj": "Sahebganj",
        "Udham Singh Nagar": "Udam Singh Nagar",
        "Dakshin Bastar Dantewada": "Dantewada",
        "Visakhapatnam": "Visakhapatanam",
        "Muktsar": "Sri Muktsar Sahib",
        "Kushinagar": "Kushi Nagar",
        "Gulbarga": "Kalaburagi",
        "Kamrup Metropolitan": "Kamrup Metro",
        "Lawangtlai": "Lawngtlai",
        "Gondiya": "Gondia",
        "Chittaurgarh": "Chittorgarh",
        "Nabarangapur": "Nabarangpur",
        "Mahbubnagar": "Mahabubnagar",
        "South Andaman": "South Andamans",
        "Barddhaman": "Purba Bardhaman",
        "West Nimar": "Khargone",
        "Jaintia Hills": "East Jaintia Hills",
        "Uttar Bastar Kanker": "Bastar",
        "Bid": "Beed",
    }

    dataframe["district_name"] = dataframe["district_name"].replace(mapping)

    #
    dataframe = dataframe.sort_values(by="date", inplace=False)
    # removing "total" values in the data
    dataframe = dataframe[dataframe["state_name"] != "Total"]
    dataframe = dataframe[dataframe["district_name"] != "Total"]
    # creating a state_code column with blank values
    dataframe["state_code"] = ""
    dataframe["district_code"] = ""
    dist_data_list = []
    # Iterating through each row to get the state_code
    for each_state in dataframe["state_name"].unique():
        print(each_state)
        filtered_data = dataframe[dataframe["state_name"] == each_state]
        filtered_data["state_code"] = get_state_lgd_code(state_name=each_state)
        # data_list.append(filtered_data)
        if get_state_lgd_code(state_name=each_state) is None:
            print("No exact match for state,", each_state)
            break
        else:
            for each_district in filtered_data["district_name"].unique():
                dist_data = filtered_data[
                    filtered_data["district_name"] == each_district
                ]
                dist_data["district_code"] = get_district_lgd_code(
                    district_name=each_district
                )
                if get_district_lgd_code(district_name=each_district) is None:
                    print("No Exact Match for district,", each_district)
                else:
                    # print("we have a match for,",each_district )
                    dist_data_list.append(dist_data)

    dataframe = pd.concat(dist_data_list)
    dataframe = dataframe[
        [
            "date",
            "state_name",
            "state_code",
            "district_name",
            "district_code",
            "nst",
            "currentLevel",
            "noOfStations",
            "gwstorage",
        ]
    ]
    dataframe.columns = [
        "date",
        "state_name",
        "state_code",
        "district_name",
        "district_code",
        "total_stations",
        "current_level",
        "no_stations_central",
        "gw_storage",
    ]
    file_name = "district_level_groundwater.csv"
    return dataframe, file_name


# %%
# creating data files as output
# state
state_info = state_level_transformer(dataset=state_dataset)
st_file_name = state_info[1]
st_data_file = state_info[0]
st_data_file.to_csv(processed + "/" + st_file_name, index=False)
# district
dist_info = district_level_transformer(dataset=state_dataset)
dist_file_name = dist_info[1]
dist_data_file = dist_info[0]
dist_data_file.to_csv(processed + "/" + dist_file_name, index=False)
