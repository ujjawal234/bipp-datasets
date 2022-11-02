import json
import re
from pathlib import Path

# import bs4 as bs
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# import lxml


"""This file is scraper for NRLM F1c form or RF fund disbursment. It involves three different processes:"""
"""1. Directory definitions and calling in the flat list of nested dictionaries."""
"""2. Scraping of data from the concerned webpages."""
"""3. Wrangling of data parsed from HTML of GP level webpage and saved as csv in concerend folders. This section is nested within step 2."""
# *****************************************************************************************************************************************************************************************#


"""PROCESS 1"""
# defining directories

time_stamp = "2022_23_June"
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp)
interim_path = Path.joinpath(dir_path, "data", "interim", time_stamp)
all_names_path = Path.joinpath(interim_path, "all_names.json")

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

# *****************************************************************************************************************************************************************************************#


# *****************************************************************************************************************************************************************************************#
"""PROCESS 2"""

"""The scraping of NRLM F1c form or RF fund disbursment begins here"""


# defining Chrome options
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": str(dir_path),
    "profile.default_content_setting_values.automatic_downloads": 1,
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--ignore-certificate-errors")


driver = webdriver.Chrome(
    ChromeDriverManager().install(), options=chrome_options
)


# fectching URL
url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDisbursementByCurrentMonthAndYear"


# looping across all names

for row in all_names:

    folder_path = Path.joinpath(
        raw_path,
        row["state_name"].strip().replace(" ", "_"),
        row["district_name"].strip().replace(" ", "_"),
        row["block_name"].strip().replace(" ", "_"),
    )

    gp_name_corrected = re.sub(
        r"[^A-Za-z0-9_]", "", row["gp_name"].strip().replace(" ", "_")
    )

    file_path = Path.joinpath(folder_path, f"{gp_name_corrected}.csv")

    if not folder_path.exists():
        folder_path.mkdir(parents=True)

    if not file_path.exists():
        print(file_path, "doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 1:

            counter += 1

            try:
                driver.get(url)

                driver.implicitly_wait(5)

                # ************************POINT OF MANUAL ITERATION********************#
                # selecting the year web element
                year_select = Select(driver.find_element(By.NAME, "year"))
                year = "2022-2023"
                year_select.select_by_value(year)

                driver.implicitly_wait(5)

                # selecting the month web element
                month_select = Select(driver.find_element(By.NAME, "month"))
                month = "06"  # June
                month_select.select_by_value(month)

                driver.implicitly_wait(5)

                # clicking submit button
                SubmitButton = driver.find_element(
                    By.XPATH,
                    "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]",
                )
                SubmitButton.click()

                driver.implicitly_wait(5)
                # *********************************************************************#

                # selecting the sate href web element
                state_page = driver.find_elements(
                    By.XPATH, "//*[@class='panel panel-default']//a"
                )
                state_page = state_page[2:]

                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):
                    if state == row["state_name"]:
                        state_dict = {state: st_href}

                driver.execute_script(state_dict[row["state_name"]])

                driver.implicitly_wait(5)

                district_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                district_row_select.select_by_value("100")

                driver.implicitly_wait(5)

                district_table = driver.find_elements(
                    By.XPATH, "//*[@id='example']//a"
                )

                district_names = [
                    x.get_attribute("text") for x in district_table
                ]

                district_hrefs = [
                    x.get_attribute("href") for x in district_table
                ]

                district_dict = {}

                for district, dist_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: dist_href}

                driver.execute_script(district_dict[row["district_name"]])

                driver.implicitly_wait(5)

                block_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                block_row_select.select_by_value("100")

                driver.implicitly_wait(5)

                block_table = driver.find_elements(
                    By.XPATH, "//*[@id='example']//a"
                )

                block_names = [x.get_attribute("text") for x in block_table]

                block_hrefs = [x.get_attribute("href") for x in block_table]

                """Checking for the NEXT button in Block list of District page"""

                while True:

                    NextButton = driver.find_element(
                        By.XPATH, '//*[@id="example_next"]'
                    )
                    next_page_class = NextButton.get_attribute("class")

                    if next_page_class != "paginate_button next disabled":

                        NextButton.click()

                        print(
                            f'NEXT Button exists for {row["state_name"]} {row["district_name"]}. Scraping block names the next page.'
                        )

                        driver.implicitly_wait(5)

                        block_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        block_names_next = [
                            x.get_attribute("text") for x in block_table
                        ]

                        block_names.extend(block_names_next)

                        block_hrefs_next = [
                            x.get_attribute("href") for x in block_table
                        ]

                        block_hrefs.extend(block_hrefs_next)

                        # driver.implicitly_wait(5)

                    else:
                        break

                block_dict = {}

                for block, block_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: block_href}

                driver.execute_script(block_dict[row["block_name"]])

                driver.implicitly_wait(5)

                gp_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                gp_row_select.select_by_value("100")

                driver.implicitly_wait(5)

                gp_table = driver.find_elements(
                    By.XPATH, "//*[@id='example']//a"
                )

                gp_hrefs = [x.get_attribute("href") for x in gp_table]
                gp_names = [x.get_attribute("text") for x in gp_table]

                """Checking for the NEXT button in GP list of block page"""

                while True:

                    NextButton = driver.find_element(
                        By.XPATH, '//*[@id="example_next"]'
                    )
                    next_page_class = NextButton.get_attribute("class")

                    if next_page_class != "paginate_button next disabled":

                        NextButton.click()

                        print(
                            f'NEXT Button exists for {row["state_name"]} {row["district_name"]} {row["block_name"]}. Scraping GP names the next page.'
                        )

                        driver.implicitly_wait(5)

                        gp_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        gp_names_next = [
                            x.get_attribute("text") for x in gp_table
                        ]
                        gp_hrefs_next = [
                            x.get_attribute("href") for x in gp_table
                        ]

                        gp_names.extend(gp_names_next)
                        gp_hrefs.extend(gp_hrefs_next)

                        driver.implicitly_wait(5)

                    else:
                        break

                gp_dict = {}

                # print(row)
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    gp = re.sub(r"[^A-Za-z0-9_]", "", gp)
                    gp = "".join(gp.split("\xa0"))

                    row["gp_name"] = re.sub(
                        r"[^A-Za-z0-9_]", "", row["gp_name"]
                    )

                    if gp == row["gp_name"]:
                        # print("")
                        gp_dict = {gp: gp_href}

                    # print(gp_dict)
                    # print(row['gp_name'])
                    # print(gp==row['gp_name'])

                """Scraping the data table for each GP"""

                print(gp_dict[row["gp_name"]])
                driver.execute_script(gp_dict[row["gp_name"]])

                driver.implicitly_wait(5)

                vill_row_select = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )

                vill_row_select.select_by_value("100")

                driver.implicitly_wait(5)

                # identifying the table element
                vill_table = driver.find_element(By.ID, "example")

                # acquiring the table html
                vill_table_html = vill_table.get_attribute("outerHTML")

                # parsing html via pandas into a df
                vill_data = pd.read_html(vill_table_html, flavor="lxml")

                # assiging to avoid list index calls
                vill_data_1 = vill_data[0]

                # renamimg columns to digits
                vill_data_1.columns = [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                ]

                # dropping mutiple unnecessary fields
                vill_data_1 = vill_data_1.drop(
                    [0, 2, 3, 4, 5, 6, 7, 14], axis=1
                )

                # renaming columns
                vill_data_1.columns = [
                    "village_name",
                    "num_new_shg",
                    "amt_new_shg",
                    "num_pre_nrlm_revived_shg",
                    "amt__pre_nrlm_revived_shg",
                    "total_num_shg",
                    "total_amt_shg",
                ]

                # subsetting to remove last observation
                vill_data_1 = vill_data_1[
                    vill_data_1["village_name"] != "Total"
                ]

                # assigning identifier variables
                vill_data_1.insert(0, "year", year)
                vill_data_1.insert(1, "month", month)
                vill_data_1.insert(2, "state", row["state_name"])
                vill_data_1.insert(3, "district", row["district_name"])
                vill_data_1.insert(4, "block", row["block_name"])
                vill_data_1.insert(5, "gp", row["gp_name"])

                vill_data_list = []

                while True:
                    # checking for next page
                    # if EC.element_to_be_clickable((By.XPATH, '//*[@id="example_next"]')):

                    try:
                        driver.find_element(
                            By.XPATH, '//*[@class="paginate_button next"]'
                        ).click()

                        # if(driver.find_element(By.XPATH, '//*[@id="example_next"]')):
                        # driver.find_element(By.XPATH, '//*[@id="example_next"]').click()

                        vill_data_list.append(vill_data_1)

                        vill_table = driver.find_element(By.ID, "example")
                        vill_table_html = vill_table.get_attribute("outerHTML")
                        vill_data = pd.read_html(
                            vill_table_html, flavor="lxml"
                        )

                        vill_data_1 = vill_data[0]

                        vill_data_1.columns = [
                            0,
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            9,
                            10,
                            11,
                            12,
                            13,
                            14,
                        ]

                        vill_data_1 = vill_data_1.drop(
                            [0, 2, 3, 4, 5, 6, 7, 14], axis=1
                        )

                        vill_data_1.columns = [
                            "village_name",
                            "num_new_shg",
                            "amt_new_shg",
                            "num_pre_nrlm_revived_shg",
                            "amt__pre_nrlm_revived_shg",
                            "total_num_shg",
                            "total_amt_shg",
                        ]

                        vill_data_1 = vill_data_1[
                            vill_data_1["village_name"] != "Total"
                        ]

                        vill_data_1.insert(0, "year", year)
                        vill_data_1.insert(1, "month", month)
                        vill_data_1.insert(2, "state", row["state_name"])
                        vill_data_1.insert(3, "district", row["district_name"])
                        vill_data_1.insert(4, "block", row["block_name"])
                        vill_data_1.insert(5, "gp", row["gp_name"])

                        vill_data_list.append(vill_data_1)

                    except NoSuchElementException:
                        # print(f"No NEXT button for {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']}")
                        break

                if len(vill_data_list) > 0:
                    vill_data_final = pd.concat(vill_data_list, axis=0)
                else:
                    vill_data_final = vill_data_1

                vill_data_final.to_csv(file_path, index=False)
                print(
                    f"Exporting {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} as csv"
                )

                break

            except (NoSuchElementException, TimeoutException, KeyError) as ex:
                print(
                    f"{ex}: Rasied at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']}"
                )

                print("Calling driver again")

            except WebDriverException as ex:
                print(
                    f"{ex}: Rasied at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']}"
                )

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {
                    "download.default_directory": str(dir_path),
                    "profile.default_content_setting_values.automatic_downloads": 1,
                }
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(
                    ChromeDriverManager().install(), options=chrome_options
                )

    else:
        print(file_path, "exists. Moving to next row.")


# driver.close()

print("Looping has ended. Scraper rests.")
