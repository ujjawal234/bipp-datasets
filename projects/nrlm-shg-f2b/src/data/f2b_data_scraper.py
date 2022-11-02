import json
import re
import time
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

time_stamp = "2022_23_April"

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp)
interim_path = Path.joinpath(dir_path, "data", "interim", time_stamp)
all_names_path = Path.joinpath(interim_path, "all_names.json")

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

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

# fetching url

url = "https://nrlm.gov.in/CommunityInvestmentFundDisbursementAction.do?methodName=showDetail"
driver.get(url)
driver.implicitly_wait(5)

# selecting the year web element
year_select = Select(driver.find_element(By.NAME, "year"))
year = "2022"
year_select.select_by_value(year)
time.sleep(2)
year_select = Select(driver.find_element(By.NAME, "toYear"))
year_select.select_by_value(year)

time.sleep(2)

# selecting the month web element
month_select = Select(driver.find_element(By.NAME, "month"))
month = "04"  # April
month_select.select_by_value(month)

time.sleep(2)
month_select = Select(driver.find_element(By.NAME, "toMonth"))
month_select.select_by_value(month)

time.sleep(2)

# clicking submit button
SubmitButton = driver.find_element(
    By.XPATH, "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]"
)
SubmitButton.click()

driver.implicitly_wait(5)

# getting the table for all the states#
all_state_path = Path.joinpath(raw_path, "all_states.csv")
if not all_state_path.exists():
    table_state = driver.find_element(
        By.XPATH, "/html/body/div[4]/form/div[2]/div/table/tbody/tr/td/table"
    )
    table_state_final = table_state.get_attribute("outerHTML")
    df = pd.read_html(table_state_final, header=3)
    print(df)
    df = df[0]
    df.set_axis(
        [
            "SNo",
            "State Name",
            "Smmu_no",
            "Smmu_amount",
            "dmmu_no",
            "dmmu_amount",
            "bmmu_no",
            "bmmu_amount",
            "vo_no",
            "vo_amount",
            "clf_no",
            "clf_amount",
            "total_district",
            "total_amt",
        ],
        axis=1,
        inplace=True,
    )
    df.dropna(axis=0, subset=["SNo"], inplace=True)
    df_clean = df[df.SNo.str.isnumeric()]
    df_clean = df_clean.drop(columns=["SNo"], axis=1)
    df_clean.insert(0, "month", month)
    df_clean.insert(0, "year", year)
    df_clean.to_csv(all_state_path, index=False)
    print("scrapped all states table.proceeding to state level scripting")

else:
    print("The csv already exists for all state_levels")

# checking how many rows there have to be scraped

# defining  variables
district_names = []
district_hrefs = []
block_names = []
block_hrefs = []
gp_names = []
gp_hrefs = []
village_names = []
village_hrefs = []
# looping through each state#

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

total_rows = len(all_names)
count_variable = 0
# the scraper for each granularity starts from here#

for row in all_names:

    count_variable += 1

    print("Scrapping " + str(count_variable) + "/" + str(total_rows))
    counter = 0

    while counter <= 2:
        try:
            counter += 1

            driver.get(url)
            driver.implicitly_wait(10)
            # selecting the year web element
            year_select = Select(driver.find_element(By.NAME, "year"))
            year = "2022"
            year_select.select_by_value(year)
            time.sleep(2)
            year_select = Select(driver.find_element(By.NAME, "toYear"))
            year = "2022"
            year_select.select_by_value(year)
            time.sleep(2)

            # selecting the month web element
            month_select = Select(driver.find_element(By.NAME, "month"))
            month = "04"  # April
            month_select.select_by_value(month)

            time.sleep(2)
            month_select = Select(driver.find_element(By.NAME, "toMonth"))
            month = "04"  # April
            month_select.select_by_value(month)

            time.sleep(2)
            # clicking submit button
            SubmitButton = driver.find_element(
                By.XPATH, "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]"
            )
            SubmitButton.click()

            driver.implicitly_wait(10)
            # FOR STATE
            state_folder_path = Path.joinpath(
                raw_path, row["state_name"].strip().replace(" ", "_")
            )
            state_name_corrected = re.sub(
                r"[^A-Za-z0-9_]",
                "",
                row["state_name"].strip().replace(" ", "_"),
            )
            state_file_path = Path.joinpath(
                state_folder_path, f"{state_name_corrected}.csv"
            )

            if not state_folder_path.exists():
                state_folder_path.mkdir(parents=True)

            if not state_file_path.exists():
                print(
                    "csv for state "
                    + state_name_corrected
                    + " doesn't exist and proceeding for scraping."
                )

                # selecting the state href web element
                state_page = driver.find_elements(
                    By.XPATH, "//*[@class='panel panel-default']//a"
                )
                state_page = state_page[2:]

                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}
                for state, st_href in zip(state_names, state_hrefs):
                    print(state, row["state_name"])
                    if state == row["state_name"]:
                        state_dict = {state: st_href}

                        driver.execute_script(state_dict[row["state_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the districts#
                        district_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        district_names = [
                            x.get_attribute("text") for x in district_table
                        ]

                        district_hrefs = [
                            x.get_attribute("href") for x in district_table
                        ]
                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + ". Scraping district names the next page."
                                )

                                driver.implicitly_wait(5)
                                # getting the names of districts and hyoerlinks in next page
                                district_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                district_names_next = [
                                    x.get_attribute("text")
                                    for x in district_table
                                ]
                                district_names.extend(district_names_next)
                                district_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in district_table
                                ]
                                district_hrefs.extend(district_hrefs_next)

                                # getting the state table

                                state_table_element_new = driver.find_element(
                                    By.XPATH,
                                    "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                                )
                                state_table_element_html_new = (
                                    state_table_element_new.get_attribute(
                                        "outerHTML"
                                    )
                                )
                                state_table_new = pd.read_html(
                                    state_table_element_html_new, header=2
                                )
                                state_table_new = state_table_new[0]
                                # state_table.append(state_table_new)

                                driver.implicitly_wait(5)
                            else:

                                break

                        # retrieving the state table
                        state_table_element = driver.find_element(
                            By.XPATH,
                            "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                        )
                        state_table_element_html = (
                            state_table_element.get_attribute("outerHTML")
                        )
                        state_table = pd.read_html(
                            state_table_element_html, header=2
                        )
                        state_table = state_table[0]
                        driver.implicitly_wait(10)
                        # storing state table as csv

                        print(state_table)
                        #####

                        state_table.drop(
                            columns=state_table.columns[-1],
                            axis=1,
                            inplace=True,
                        )
                        state_table.set_axis(
                            [
                                "SNo",
                                "District Name",
                                "Smmu_no",
                                "Smmu_amount",
                                "dmmu_no",
                                "dmmu_amount",
                                "bmmu_no",
                                "bmmu_amount",
                                "vo_no",
                                "vo_amount",
                                "clf_no",
                                "clf_amount",
                                "total distict number",
                                "total district amount",
                            ],
                            axis=1,
                            inplace=True,
                        )
                        state_table.dropna(
                            axis=0, subset=["SNo"], inplace=True
                        )
                        state_table.drop(columns=["SNo"], axis=1, inplace=True)

                        state_table.insert(0, "month", month)
                        state_table.insert(0, "year", year)
                        state_table.insert(0, "state", row["state_name"])
                        print(state_table)
                        # storing state table as csv
                        state_path = Path.joinpath(
                            raw_path, state.replace(" ", "_")
                        )
                        if not state_path.exists():
                            Path.mkdir(state_path, parents=True)

                        state_table.to_csv(
                            Path.joinpath(
                                state_path, state.replace(" ", "_") + ".csv"
                            ),
                            index=False,
                        )
                        print(
                            "Scraped "
                            + state
                            + " table. Moving to next scraping task."
                        )

            else:
                print(
                    "csv for state "
                    + state_name_corrected
                    + " exists. Moving to next scraping task."
                )
                state_page = driver.find_elements(
                    By.XPATH, "//*[@class='panel panel-default']//a"
                )
                state_page = state_page[2:]

                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}
                for state, st_href in zip(state_names, state_hrefs):
                    # print (state, row["state_name"])
                    if state == row["state_name"]:
                        state_dict = {state: st_href}

                        driver.execute_script(state_dict[row["state_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)
                        # collecitng the names and hyperlinks of all the districts#
                        district_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        district_names = [
                            x.get_attribute("text") for x in district_table
                        ]

                        district_hrefs = [
                            x.get_attribute("href") for x in district_table
                        ]
                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + ". Scraping district names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of districts and hyoerlinks in next page
                                district_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                district_names_next = [
                                    x.get_attribute("text")
                                    for x in district_table
                                ]
                                district_names.extend(district_names_next)
                                district_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in district_table
                                ]
                                district_hrefs.extend(district_hrefs_next)

                            else:

                                break

            # FOR DISTRICT

            district_dict = {}
            district_folder_path = Path.joinpath(
                state_folder_path,
                row["district_name"].strip().replace(" ", "_"),
            )
            district_name_corrected = re.sub(
                r"[^A-Za-z0-9_]",
                "",
                row["district_name"].strip().replace(" ", "_"),
            )
            district_file_path = Path.joinpath(
                district_folder_path, f"{district_name_corrected}.csv"
            )
            if not district_folder_path.exists():
                district_folder_path.mkdir(parents=True)

            if not district_file_path.exists():
                print(
                    "csv for district "
                    + district_name_corrected
                    + " doesn't exist and proceeding for scraping."
                )

                for district, dist_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: dist_href}
                        driver.execute_script(
                            district_dict[row["district_name"]]
                        )
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the blocks#
                        block_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        block_names = [
                            x.get_attribute("text") for x in block_table
                        ]

                        block_hrefs = [
                            x.get_attribute("href") for x in block_table
                        ]

                        # retrieving the district table
                        district_table_element = driver.find_element(
                            By.XPATH,
                            "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                        )
                        district_table_element_html = (
                            district_table_element.get_attribute("outerHTML")
                        )
                        district_table = pd.read_html(
                            district_table_element_html, header=2
                        )
                        district_table = district_table[0]
                        print(district_table)
                        driver.implicitly_wait(10)

                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of blocks and hyoerlinks in next page
                                block_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                block_names_next = [
                                    x.get_attribute("text")
                                    for x in block_table
                                ]
                                block_names.extend(block_names_next)
                                block_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in block_table
                                ]
                                block_hrefs.extend(block_hrefs_next)

                                # getting the district table

                                district_table_element_new = driver.find_element(
                                    By.XPATH,
                                    "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                                )
                                district_table_element_html_new = (
                                    district_table_element_new.get_attribute(
                                        "outerHTML"
                                    )
                                )
                                district_table_new = pd.read_html(
                                    district_table_element_html_new, header=2
                                )
                                district_table_new = district_table_new[0]
                                district_table.append(district_table_new)

                                driver.implicitly_wait(5)
                            else:

                                break

                        # storing district table as csv
                        district_table.drop(
                            columns=district_table.columns[-1],
                            axis=1,
                            inplace=True,
                        )
                        district_table.set_axis(
                            [
                                "SNo",
                                "Block Name",
                                "Smmu_no",
                                "Smmu_amount",
                                "dmmu_no",
                                "dmmu_amount",
                                "bmmu_no",
                                "bmmu_amount",
                                "vo_no",
                                "vo_amount",
                                "clf_no",
                                "clf_amount",
                                "total distict number",
                                "total district amount",
                            ],
                            axis=1,
                            inplace=True,
                        )
                        district_table.dropna(
                            axis=0, subset=["SNo"], inplace=True
                        )
                        district_table.drop(
                            columns=["SNo"], axis=1, inplace=True
                        )

                        district_table.insert(0, "month", month)
                        district_table.insert(0, "year", year)
                        district_table.insert(
                            0, "district", row["district_name"]
                        )
                        district_table.insert(0, "state", row["state_name"])
                        district_path = Path.joinpath(
                            state_folder_path, district.replace(" ", "_")
                        )
                        if not district_path.exists():
                            Path.mkdir(district_path, parents=True)
                        print(district_table)
                        district_table.to_csv(
                            Path.joinpath(
                                district_path,
                                district.replace(" ", "_") + ".csv",
                            ),
                            index=False,
                        )
                        print(
                            "Scraped "
                            + row["state_name"]
                            + district
                            + " table. Moving to next scraping task."
                        )

            else:
                print(
                    "csv for state  "
                    + state_name_corrected
                    + "district -"
                    + district_name_corrected
                    + "exists. Moving to next scraping task."
                )
                for district, dist_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: dist_href}
                        driver.execute_script(
                            district_dict[row["district_name"]]
                        )
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the blocks#
                        block_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        block_names = [
                            x.get_attribute("text") for x in block_table
                        ]

                        block_hrefs = [
                            x.get_attribute("href") for x in block_table
                        ]
                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of blocks and hyoerlinks in next page
                                block_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                block_names_next = [
                                    x.get_attribute("text")
                                    for x in block_table
                                ]
                                block_names.extend(block_names_next)
                                block_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in block_table
                                ]
                                block_hrefs.extend(block_hrefs_next)
                            else:

                                break

            # FOR BLOCKS

            block_dict = {}
            block_folder_path = Path.joinpath(
                district_folder_path,
                row["block_name"].strip().replace(" ", "_"),
            )
            block_name_corrected = re.sub(
                r"[^A-Za-z0-9_]",
                "",
                row["block_name"].strip().replace(" ", "_"),
            )
            block_file_path = Path.joinpath(
                block_folder_path, f"{block_name_corrected}.csv"
            )
            if not block_folder_path.exists():
                block_folder_path.mkdir(parents=True)

            if not block_file_path.exists():
                print(
                    "csv for block "
                    + block_name_corrected
                    + " doesn't exist and proceeding for scraping."
                )

                for block, block_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: block_href}
                        driver.execute_script(block_dict[row["block_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the gps#
                        gp_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        gp_names = [x.get_attribute("text") for x in gp_table]

                        gp_hrefs = [x.get_attribute("href") for x in gp_table]

                        # retrieving the block table
                        block_table_element = driver.find_element(
                            By.XPATH,
                            "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                        )
                        block_table_element_html = (
                            block_table_element.get_attribute("outerHTML")
                        )
                        block_table = pd.read_html(
                            block_table_element_html, header=2
                        )
                        block_table = block_table[0]
                        print(block_table)

                        driver.implicitly_wait(10)

                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of gps and hyoerlinks in next page
                                gp_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                gp_names_next = [
                                    x.get_attribute("text") for x in gp_table
                                ]
                                gp_names.extend(gp_names_next)
                                gp_hrefs_next = [
                                    x.get_attribute("href") for x in gp_table
                                ]
                                gp_hrefs.extend(gp_hrefs_next)

                                # getting the block table

                                block_table_element_new = driver.find_element(
                                    By.XPATH,
                                    "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                                )
                                block_table_element_html_new = (
                                    block_table_element_new.get_attribute(
                                        "outerHTML"
                                    )
                                )
                                block_table_new = pd.read_html(
                                    block_table_element_html_new, header=2
                                )
                                block_table_new = block_table_new[0]
                                block_table.append(block_table_new)

                                driver.implicitly_wait(5)
                            else:

                                break

                        # storing block table as csv
                        block_table.drop(
                            columns=block_table.columns[-1],
                            axis=1,
                            inplace=True,
                        )
                        block_table.set_axis(
                            [
                                "SNo",
                                "GP Name",
                                "Smmu_no",
                                "Smmu_amount",
                                "dmmu_no",
                                "dmmu_amount",
                                "bmmu_no",
                                "bmmu_amount",
                                "vo_no",
                                "vo_amount",
                                "clf_no",
                                "clf_amount",
                                "total block number",
                                "total block amount",
                            ],
                            axis=1,
                            inplace=True,
                        )
                        block_table.dropna(
                            axis=0, subset=["SNo"], inplace=True
                        )
                        block_table.drop(columns=["SNo"], axis=1, inplace=True)

                        block_table.insert(0, "month", month)
                        block_table.insert(0, "year", year)
                        block_table.insert(0, "block", row["block_name"])

                        block_table.insert(0, "district", row["district_name"])
                        block_table.insert(0, "state", row["state_name"])
                        block_path = Path.joinpath(
                            district_folder_path, block.replace(" ", "_")
                        )
                        if not block_path.exists():
                            Path.mkdir(block_path, parents=True)
                        print(block_table)
                        block_table.to_csv(
                            Path.joinpath(
                                block_path, block.replace(" ", "_") + ".csv"
                            ),
                            index=False,
                        )
                        print(
                            "Scraped "
                            + row["state_name"]
                            + row["district_name"]
                            + block
                            + " table. Moving to next scraping task."
                        )

            else:
                print(
                    "csv for state  "
                    + state_name_corrected
                    + "district -"
                    + district_name_corrected
                    + "block"
                    + block_name_corrected
                    + "exists. Moving to next scraping task."
                )
                for block, block_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: block_href}
                        driver.execute_script(block_dict[row["block_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the gps#
                        gp_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        gp_names = [x.get_attribute("text") for x in gp_table]

                        gp_hrefs = [x.get_attribute("href") for x in gp_table]
                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of gps and hyoerlinks in next page
                                gp_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                gp_names_next = [
                                    x.get_attribute("text") for x in gp_table
                                ]
                                gp_names.extend(gp_names_next)
                                gp_hrefs_next = [
                                    x.get_attribute("href") for x in gp_table
                                ]
                                gp_hrefs.extend(gp_hrefs_next)
                            else:

                                break

            # FOR GP

            gp_dict = {}
            gp_folder_path = Path.joinpath(
                block_folder_path, row["gp_name"].strip().replace(" ", "_")
            )
            gp_name_corrected = re.sub(
                r"[^A-Za-z0-9_]", "", row["gp_name"].strip().replace(" ", "_")
            )
            gp_file_path = Path.joinpath(
                gp_folder_path, f"{gp_name_corrected}.csv"
            )
            if not gp_folder_path.exists():
                gp_folder_path.mkdir(parents=True)

            if not gp_file_path.exists():
                print(
                    "csv for GP  "
                    + gp_name_corrected
                    + "   doesn't exist and proceeding for scraping."
                )
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        gp_dict = {gp: gp_href}
                        driver.execute_script(gp_dict[row["gp_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the villages#
                        village_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        village_names = [
                            x.get_attribute("text") for x in village_table
                        ]

                        village_hrefs = [
                            x.get_attribute("href") for x in village_table
                        ]

                        # retrieving the GP table
                        gp_table_element = driver.find_element(
                            By.XPATH,
                            "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                        )
                        gp_table_element_html = gp_table_element.get_attribute(
                            "outerHTML"
                        )
                        gp_table = pd.read_html(
                            gp_table_element_html, header=2
                        )
                        gp_table = gp_table[0]
                        print(gp_table)

                        driver.implicitly_wait(10)

                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of villages and hyperlinks in next page
                                village_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                village_names_next = [
                                    x.get_attribute("text")
                                    for x in village_table
                                ]
                                village_names.extend(village_names_next)
                                village_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in village_table
                                ]
                                village_hrefs.extend(village_hrefs_next)

                                # getting the block table

                                gp_table_element_new = driver.find_element(
                                    By.XPATH,
                                    "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                                )
                                gp_table_element_html_new = (
                                    gp_table_element_new.get_attribute(
                                        "outerHTML"
                                    )
                                )
                                gp_table_new = pd.read_html(
                                    gp_table_element_html_new, header=2
                                )
                                gp_table_new = gp_table_new[0]
                                gp_table.append(gp_table_new)

                                driver.implicitly_wait(5)
                            else:

                                break

                        # storing GP table as csv
                        gp_table.drop(
                            columns=gp_table.columns[-1], axis=1, inplace=True
                        )
                        gp_table.set_axis(
                            [
                                "SNo",
                                "Village Name",
                                "Smmu_no",
                                "Smmu_amount",
                                "dmmu_no",
                                "dmmu_amount",
                                "bmmu_no",
                                "bmmu_amount",
                                "vo_no",
                                "vo_amount",
                                "clf_no",
                                "clf_amount",
                                "total distinct number",
                                "total block amount",
                            ],
                            axis=1,
                            inplace=True,
                        )
                        gp_table.dropna(axis=0, subset=["SNo"], inplace=True)
                        gp_table.drop(columns=["SNo"], axis=1, inplace=True)

                        gp_table.insert(0, "year", year)
                        gp_table.insert(0, "month", month)
                        gp_table.insert(0, "gp", row["gp_name"])
                        gp_table.insert(0, "block", row["block_name"])
                        gp_table.insert(0, "district", row["district_name"])
                        gp_table.insert(0, "state", row["state_name"])
                        gp_path = Path.joinpath(
                            block_folder_path, gp_name_corrected
                        )
                        if not gp_path.exists():
                            Path.mkdir(gp_path, parents=True)
                        print(gp_table)
                        gp_table.to_csv(
                            Path.joinpath(gp_path, f"{gp_name_corrected}.csv"),
                            index=False,
                        )
                        print(
                            "Scraped "
                            + row["state_name"]
                            + " "
                            + row["district_name"]
                            + " "
                            + row["block_name"]
                            + " "
                            + gp
                            + " table. Moving to next scraping task."
                        )

            else:
                print(
                    "csv for state  "
                    + state_name_corrected
                    + "district -"
                    + district_name_corrected
                    + "block  "
                    + block_name_corrected
                    + "gp "
                    + gp_name_corrected
                    + "exists. Moving to next scraping task."
                )
                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        gp_dict = {gp: gp_href}
                        driver.execute_script(gp_dict[row["gp_name"]])
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # collecitng the names and hyperlinks of all the villages#
                        village_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        village_names = [
                            x.get_attribute("text") for x in village_table
                        ]

                        village_hrefs = [
                            x.get_attribute("href") for x in village_table
                        ]
                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the names of villages and hyperlinks in next page
                                village_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )
                                village_names_next = [
                                    x.get_attribute("text")
                                    for x in village_table
                                ]
                                village_names.extend(village_names_next)
                                village_hrefs_next = [
                                    x.get_attribute("href")
                                    for x in village_table
                                ]
                                village_hrefs.extend(village_hrefs_next)
                            else:

                                break
            # FOR VILLAGES
            village_dict = {}
            village_folder_path = Path.joinpath(
                gp_folder_path, row["village_name"].strip().replace(" ", "_")
            )
            village_name_corrected = re.sub(
                r"[^A-Za-z0-9_]",
                "",
                row["village_name"].strip().replace(" ", "_"),
            )
            village_file_path = Path.joinpath(
                village_folder_path, f"{village_name_corrected}.csv"
            )
            if not village_folder_path.exists():
                village_folder_path.mkdir(parents=True)

            if not village_file_path.exists():
                print(
                    "csv for village "
                    + village_name_corrected
                    + " doesn't exist and proceeding for scraping."
                )

                for village, village_href in zip(village_names, village_hrefs):
                    if village == row["village_name"]:
                        village_dict = {village: village_href}
                        driver.execute_script(
                            village_dict[row["village_name"]]
                        )
                        driver.implicitly_wait(10)
                        # selecting option to view 100 pages
                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")
                        driver.implicitly_wait(10)

                        # retrieving the GP table
                        village_table_element = driver.find_element(
                            By.XPATH,
                            "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                        )
                        village_table_element_html = (
                            village_table_element.get_attribute("outerHTML")
                        )
                        village_table = pd.read_html(
                            village_table_element_html, header=1
                        )
                        village_table = village_table[0]
                        print(village_table)

                        driver.implicitly_wait(10)

                        while True:

                            NextButton = driver.find_element(
                                By.XPATH, '//*[@id="example_next"]'
                            )
                            next_page_class = NextButton.get_attribute("class")
                            if (
                                next_page_class
                                != "paginate_button next disabled"
                            ):
                                NextButton.click()
                                print(
                                    "NEXT Button exists for "
                                    + state
                                    + district
                                    + ". Scraping block names the next page."
                                )

                                driver.implicitly_wait(5)

                                # getting the block table

                                village_table_element_new = driver.find_element(
                                    By.XPATH,
                                    "/html/body/div[4]/form/div[3]/div/table/tbody/tr/td/div/div/table",
                                )
                                village_table_element_html_new = (
                                    village_table_element_new.get_attribute(
                                        "outerHTML"
                                    )
                                )
                                village_table_new = pd.read_html(
                                    village_table_element_html_new, header=1
                                )
                                village_table_new = village_table_new[0]
                                village_table.append(village_table_new)

                                driver.implicitly_wait(5)

                            else:

                                break

                        # storing village table as csv

                        village_table.set_axis(
                            [
                                "SNo",
                                "SHG Name",
                                "Disbursed_by",
                                "fund_type",
                                "source_of_fund",
                                "bank",
                                "branch",
                                "acc_no",
                                "amount",
                                "mode_of_payment",
                                "payref_no",
                                "release_date",
                                "remarks",
                            ],
                            axis=1,
                            inplace=True,
                        )
                        village_table.dropna(
                            axis=0, subset=["SNo"], inplace=True
                        )
                        village_table.drop(
                            columns=["SNo"], axis=1, inplace=True
                        )

                        village_table.insert(0, "month", month)
                        village_table.insert(0, "year", year)
                        village_table.insert(0, "village", row["village_name"])
                        village_table.insert(0, "gp", row["gp_name"])
                        village_table.insert(0, "block", row["block_name"])
                        village_table.insert(
                            0, "district", row["district_name"]
                        )
                        village_table.insert(0, "state", row["state_name"])
                        village_path = Path.joinpath(
                            gp_folder_path, village.replace(" ", "_")
                        )
                        if not village_path.exists():
                            Path.mkdir(village_path, parents=True)
                        print(village_table)
                        village_table.to_csv(
                            Path.joinpath(
                                village_path,
                                village_name_corrected.replace(" ", "_")
                                + ".csv",
                            ),
                            index=False,
                        )
                        print(
                            "Scraped "
                            + row["state_name"]
                            + " "
                            + row["district_name"]
                            + " "
                            + row["block_name"]
                            + " "
                            + row["gp_name"]
                            + " "
                            + village
                            + " table. Moving to next scraping task."
                        )

            else:
                print(
                    "csv for state  "
                    + state_name_corrected
                    + "district -"
                    + district_name_corrected
                    + "block  "
                    + block_name_corrected
                    + "gp "
                    + gp_name_corrected
                    + "village "
                    + village_name_corrected
                    + "exists. Moving to next scraping task."
                )

        except (NoSuchElementException, TimeoutException, KeyError) as ex:
            # print("MIXIE")
            print(
                f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['village_name']}"
            )

            print("Calling driver again")

            if counter == 2:
                break

        except WebDriverException as ex:
            # print("WEBBIE")
            print(
                f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['village_name']}"
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

driver.close()

print("Looping has ended. Scraper rests.")
