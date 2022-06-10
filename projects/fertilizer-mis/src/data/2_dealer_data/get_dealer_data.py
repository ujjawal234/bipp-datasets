# script to download statewise dealer data using selenium webdriver

import os

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)

# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions

# from selenium.webdriver.support.expected_conditions import staleness_of
from selenium.webdriver.support.ui import Select, WebDriverWait

options = (
    webdriver.ChromeOptions()
)  # used in order to change download directory
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": r"bipp-datasets\projects\fertilizer-mis\data\raw\2_dealer_data"
    },
)
driver = webdriver.Chrome(
    executable_path=r"C:\Users\91987\Downloads\chromedriver_win32\chromedriver.exe",
    chrome_options=options,
)
driver.get(
    "https://reports.dbtfert.nic.in/mfmsReports/getDealerInfoExcel"
)  # go to homepage
drp_state = Select(
    driver.find_element_by_id("parameterStateName")
)  # dropdown element which has the list of states
no_of_states = len(drp_state.options)  # counts the no. of states

# loop to download all files statewise

for x in range(1, no_of_states):  # iterates through the number of states
    ignored_exceptions = (
        NoSuchElementException,
        StaleElementReferenceException,
    )
    your_element = WebDriverWait(
        driver, 5, ignored_exceptions=ignored_exceptions
    ).until(
        expected_conditions.presence_of_element_located(
            (By.ID, "parameterStateName")
        )
    )
    your_element = WebDriverWait(
        driver, 5, ignored_exceptions=ignored_exceptions
    ).until(
        expected_conditions.presence_of_element_located(
            (By.ID, "parameterDistrictName")
        )
    )
    drp_state = Select(
        driver.find_element_by_id("parameterStateName")
    )  # to select the states dropdown list
    drp_state.select_by_index(x)  # selects each state by index
    driver.find_element_by_id(
        "exporttoexcel"
    ).click()  # clicks on the download button


i = -2  # iterator used for old file names
drp_state = Select(
    driver.find_element_by_id("parameterStateName")
)  # to select the state dropdown list
states_list = drp_state.options

# loop to rename all files statewise

for state in states_list:
    i = i + 1  # used for old file names
    drp_state = Select(
        driver.find_element_by_id("parameterStateName")
    )  # to select the state dropdown list
    state_name = (
        state.text
    )  # state_name stores the visible text of the options
    new_file_name = state_name + ".csv"
    if (
        i == 0
    ):  # i=-1 is for the option select state, i=0 is for the first file
        old_file_name = "Report.csv"
    elif i >= 1:
        old_file_name = "Report ({}).csv".format(i)
    if i >= 0:
        old_file = os.path.join(
            r"bipp-datasets\projects\fertilizer-mis\data\raw\2_dealer_data",
            old_file_name,
        )
        new_file = os.path.join(
            r"bipp-datasets\projects\fertilizer-mis\data\raw\2_dealer_data",
            new_file_name,
        )
        os.rename(old_file, new_file)

# end of script
