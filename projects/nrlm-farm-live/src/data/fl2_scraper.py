import json
import re
from pathlib import Path
from time import sleep

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    ElementNotSelectableException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By

# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

"""This file is scraper for NRLM FL2 form or Farm Livelihoods Indicators:Month wise report. It involves three different processes:"""
"""1. Directory definitions and calling in the flat list of nested dictionaries."""
"""2. Scraping of data from the concerned webpages."""
"""3. Wrangling of data parsed from HTML of Block level webpage and saved as csv in concerend folders. This section is nested within step 2."""
# *****************************************************************************************************************************************************************************************#

"""PROCESS 1"""
# defining directories
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")
all_names_path = Path.joinpath(interim_path, "jsons", "all_names_extended_new.json")


with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

# use all_names_extended.json when 2022-2023 webpage is updated with all the months in dropdown lists

# *****************************************************************************************************************************************************************************************#


# *****************************************************************************************************************************************************************************************#
"""PROCESS 2"""

"""The scraping of NRLM FL2 form begins here"""

# defining Chrome options
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": str(dir_path),
    "profile.default_content_setting_values.automatic_downloads": 1,
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--ignore-certificate-errors")

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)


# fetching url
url = "https://nrlm.gov.in/FLMPRIndicatorsWiseAction.do?methodName=showDetail"

# looping across all names

for row in all_names:

    folder_path = Path.joinpath(
        raw_path,
        row["year"].strip().replace(" ", "_"),
        row["month"].strip().replace(" ", "_"),
        row["state_name"].strip().replace(" ", "_"),
        row["district_name"].strip().replace(" ", "_"),
    )

    block_name_corrected = re.sub(
        r"[^A-Za-z0-9_]", "", row["block_name"].strip().replace(" ", "_")
    )

    file_path = Path.joinpath(folder_path, f"{block_name_corrected}.csv")

    if not folder_path.exists():
        folder_path.mkdir(parents=True)

    if not file_path.exists():
        print(file_path, "doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1

                driver.get(url)
                sleep(5)

                # # defining explicit conditional wait
                # wait = WebDriverWait(driver, 10)
                # wait.until(EC.visibility_of_element_located, (By.XPATH, '//*[@id="yearId"]'))

                # selecting adequate values from dropdown lists
                years_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="yearId"]')
                )
                years_select.select_by_value(row["year"])
                sleep(1)

                from_month_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="fmonth"]')
                )
                from_month_select.select_by_value(row["month_code"])
                sleep(1)

                to_month_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="tmonth"]')
                )
                to_month_select.select_by_value(row["month_code"])
                sleep(1)

                state_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="stateId"]')
                )
                state_select.select_by_value(row["state_code"])
                sleep(1)

                district_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="districtId"]')
                )
                district_select.select_by_value(row["district_code"])
                sleep(1)

                block_select = Select(
                    driver.find_element(By.XPATH, '//*[@id="blockId"]')
                )
                block_select.select_by_value(row["block_code"])
                sleep(1)

                SubmitButton = driver.find_element(
                    By.XPATH, "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]"
                )
                SubmitButton.click()
                sleep(3)

                """PROCESS 3"""

                """The wrangling of NRLM FL2 block level page begins here"""

                # making all the entires visible on the page
                entries_select = Select(driver.find_element(By.NAME, "example_length"))
                entries_select.select_by_value("100")
                sleep(1)

                # Parsing the data table from HTML
                block_table = driver.find_element(
                    By.XPATH, "/html/body/div[4]/form/div[2]/div/div/div[3]/div[2]"
                )
                block_table_html = block_table.get_attribute("outerHTML")
                block_data = pd.read_html(block_table_html, flavor="lxml", header=1)

                block_data = block_data[1]

                col_names = [
                    x.replace(" ", "_").lower().strip() for x in block_data.columns
                ]
                col_names = [
                    re.sub(r"[^A-Za-z0-9_]", "", x.replace("-", "_")) for x in col_names
                ]
                col_names[0] = "indicator_number"

                block_data.columns = col_names

                # assigning identifier variables
                block_data.insert(0, "year", row["year"])
                block_data.insert(1, "month", row["month"])
                block_data.insert(2, "state", row["state_name"])
                block_data.insert(3, "district", row["district_name"])
                block_data.insert(4, "block", row["block_name"])

                block_data.to_csv(file_path, index=False)
                print(
                    f"Exporting {row['year']} {row['month']} {row['state_name']} {row['district_name']} {row['block_name']} as csv"
                )

                break

            except (
                NoSuchElementException,
                TimeoutException,
                StaleElementReferenceException,
                ElementNotSelectableException,
            ) as ex:
                print(f"{ex}has been raised")
                # print("alphe")

            except WebDriverException as ex:
                print(f"{ex}has been raised")
                # print("webbie")

    else:
        print(f"{file_path} exists. Moving to the next block")


print("Scraping has ended. Scraper rests.")
driver.close()
