"""the name scraper script for nrlm f1b data."""

# importing the necessary libraries

import json
import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# defining directories

time_stamp = "2022_July"
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp, "jsons")
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = Path.joinpath(dir_path, "data", "external")

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

# fetching url

url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDetail"
driver.get(url)
driver.implicitly_wait(2)

# the first year and month

selectyear1 = Select(driver.find_element(By.ID, "yearId"))
year1 = "2022"
selectyear1.select_by_value(year1)
time.sleep(15)

selectmonth1 = Select(driver.find_element(By.ID, "month"))
month1 = "07"
selectmonth1.select_by_value(month1)
driver.implicitly_wait(10)

# the second year and month

selectyear2 = Select(driver.find_element(By.ID, "yearIdd"))
year2 = "2022"
selectyear2.select_by_value(year2)
driver.implicitly_wait(10)

selectmonth2 = Select(driver.find_element(By.ID, "toMonth"))
month2 = "07"
selectmonth2.select_by_value(month2)
driver.implicitly_wait(10)

# clicking submit

driver.find_element(
    By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]'
).click()
driver.implicitly_wait(5)


# **************************************************************************************#

# selecting the states with hyperlink

state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
state_names = [x.get_attribute("text") for x in state_page]
state_hrefs = [x.get_attribute("href") for x in state_page]

# traversing through state list

for state, st_href in zip(state_names, state_hrefs):

    print("State: " + state, end="\n")

    # checking if state path exists or not

    state_path = Path.joinpath(raw_path, state.lower().replace(" ", "_"))
    if not state_path.exists():
        print("Making the state path for the state " + state + "...", end="\n")
        state_path.mkdir(parents=True)

    driver.execute_script(st_href)

    driver.implicitly_wait(2)

    # selecting option to view 100 pages

    no_of_pages = Select(
        driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select')
    )
    no_of_pages.select_by_value("100")

    driver.implicitly_wait(2)

    # select the districts with hyperlink

    district_table = driver.find_elements(By.XPATH, '//*[@id="example"]//a')
    district_names = [x.get_attribute("text") for x in district_table]
    district_hrefs = [x.get_attribute("href") for x in district_table]

    # checking for next button

    while True:
        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
        next_page_class = NextButton.get_attribute("class")
        if next_page_class != "paginate_button next disabled":
            NextButton.click()

            driver.implicitly_wait(2)

            print(
                "NEXT button exists for "
                + state
                + ". Scraping district names the next page...",
                end="\n",
            )

            # selecting option to view 100 pages

            no_of_pages = Select(
                driver.find_element(
                    By.XPATH, '//*[@id="example_length"]/label/select'
                )
            )
            no_of_pages.select_by_value("100")

            driver.implicitly_wait(2)

            # select the districts with hyperlink

            district_table = driver.find_elements(
                By.XPATH, "//*[@id='example']//a"
            )
            district_names_next = [
                x.get_attribute("text") for x in district_table
            ]
            district_names.extend(district_names_next)
            block_hrefs_next = [
                x.get_attribute("href") for x in district_table
            ]
            district_hrefs.extend(block_hrefs_next)

        else:

            break

    # traversing through district names

    for district, di_href in zip(district_names, district_hrefs):

        # try block begins
        try:

            print("\n\n" + "District: " + district, end="\n")

            # defining json

            json_name = ".".join([district.lower().replace(" ", "_"), "json"])
            district_json_path = Path.joinpath(raw_path, state, json_name)

            # checking if json exists

            if not district_json_path.exists():

                print(
                    "json for "
                    + district
                    + " doesn't exist. Proceeding for scraping...",
                    end="\n",
                )

                # defining a list for storing all jsons

                json_list = []

                driver.execute_script(di_href)

                driver.implicitly_wait(2)

                # selecting option to view 100 pages

                no_of_pages = Select(
                    driver.find_element(
                        By.XPATH, '//*[@id="example_length"]/label/select'
                    )
                )
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(2)

                # select the blocks with hyperlink

                block_table = driver.find_elements(
                    By.XPATH, '//*[@id="example"]//a'
                )
                block_names = [x.get_attribute("text") for x in block_table]
                block_hrefs = [x.get_attribute("href") for x in block_table]

                # checking for next button

                while True:
                    NextButton = driver.find_element(
                        By.XPATH, '//*[@id="example_next"]'
                    )
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()

                        driver.implicitly_wait(2)

                        print(
                            "NEXT button exists for "
                            + district
                            + ". Scraping block names the next page...",
                            end="\n",
                        )

                        # selecting option to view 100 pages

                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")

                        driver.implicitly_wait(2)

                        # select the blocks with hyperlink

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

                    else:

                        break

                # traversing through block names

                for block, bl_href in zip(block_names, block_hrefs):

                    # sub-try block begins

                    try:

                        print("Block " + block, end="\n")

                        driver.execute_script(bl_href)

                        driver.implicitly_wait(2)

                        # selecting option to view 100 pages

                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )
                        no_of_pages.select_by_value("100")

                        driver.implicitly_wait(2)

                        # select the gps with hyperlink

                        gp_table = driver.find_elements(
                            By.XPATH, '//*[@id="example"]//a'
                        )
                        gp_names = [x.get_attribute("text") for x in gp_table]
                        gp_hrefs = [x.get_attribute("href") for x in gp_table]

                        # checking for next button

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

                                driver.implicitly_wait(2)

                                print(
                                    "NEXT button exists for "
                                    + block
                                    + ". Scraping grampanchayat names the next page...",
                                    end="\n",
                                )

                                # selecting option to view 100 pages

                                no_of_pages = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )
                                no_of_pages.select_by_value("100")

                                driver.implicitly_wait(2)

                                # select the gps with hyperlink

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

                        # traversing gp names

                        for gp, gp_href in zip(gp_names, gp_hrefs):

                            # sub-sub-try block begins
                            try:

                                print("Grampanchayat " + gp, end="\n")

                                driver.execute_script(gp_href)

                                driver.implicitly_wait(2)

                                # selecting option to view 100 pages

                                no_of_pages = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )
                                no_of_pages.select_by_value("100")

                                driver.implicitly_wait(2)

                                # select the villages with hyperlink

                                village_table = driver.find_elements(
                                    By.XPATH, '//*[@id="example"]//a'
                                )
                                village_names = [
                                    x.get_attribute("text")
                                    for x in village_table
                                ]
                                village_hrefs = [
                                    x.get_attribute("href")
                                    for x in village_table
                                ]

                                # checking for next button

                                while True:
                                    NextButton = driver.find_element(
                                        By.XPATH, '//*[@id="example_next"]'
                                    )
                                    next_page_class = NextButton.get_attribute(
                                        "class"
                                    )
                                    if (
                                        next_page_class
                                        != "paginate_button next disabled"
                                    ):
                                        NextButton.click()

                                        driver.implicitly_wait(2)

                                        print(
                                            "NEXT button exists for "
                                            + gp
                                            + ". Scraping village names the next page...",
                                            end="\n",
                                        )

                                        # selecting option to view 100 pages

                                        no_of_pages = Select(
                                            driver.find_element(
                                                By.XPATH,
                                                '//*[@id="example_length"]/label/select',
                                            )
                                        )
                                        no_of_pages.select_by_value("100")

                                        driver.implicitly_wait(2)

                                        # select the villages with hyperlink

                                        village_table = driver.find_elements(
                                            By.XPATH, "//*[@id='example']//a"
                                        )
                                        village_names_next = [
                                            x.get_attribute("text")
                                            for x in village_table
                                        ]
                                        village_names.extend(
                                            village_names_next
                                        )
                                        village_hrefs_next = [
                                            x.get_attribute("href")
                                            for x in village_table
                                        ]
                                        village_hrefs.extend(
                                            village_hrefs_next
                                        )

                                    else:

                                        break

                                if len(village_names) == 0:

                                    print("Village:  NOT_FOUND", end="\n")

                                    # ensuring the village name is in english

                                    if (
                                        (str.isascii(district))
                                        and (str.isascii(block))
                                        and (str.isascii(gp))
                                    ):

                                        # creating village dictionary

                                        village_dict = {
                                            "state_name": state,
                                            "district_name": district,
                                            "block_name": block,
                                            "gp_name": gp,
                                            "village_name": "NOT_FOUND",
                                        }
                                        json_list.append(village_dict)

                                    else:

                                        print(
                                            "Devanagari in name at {state}, {district}, {block}, {gp}, {village}. Hence, moving on to next village...",
                                            end="\n",
                                        )

                                else:

                                    for village in village_names:

                                        print("Village: " + village, end="\n")

                                        # ensuring the village name is in english

                                        if (
                                            (str.isascii(district))
                                            and (str.isascii(block))
                                            and (str.isascii(gp))
                                            and (str.isascii(village))
                                        ):

                                            # creating village dictionary

                                            village_dict = {
                                                "state_name": state,
                                                "district_name": district,
                                                "block_name": block,
                                                "gp_name": gp,
                                                "village_name": village,
                                            }
                                            json_list.append(village_dict)

                                        else:

                                            print(
                                                "Devanagari in name at {state}, {district}, {block}, {gp}, {village}. Hence, moving on to next village...",
                                                end="\n",
                                            )

                            # sub-sub-try block ends

                            # sub-sub-except block begins

                            except NoSuchElementException:

                                print(
                                    f"No Such Element raised at {state} {district} {block} {gp}"
                                )

                                driver.get(url)

                                driver.implicitly_wait(2)

                                # the first year and month

                                selectyear1 = Select(
                                    driver.find_element(By.ID, "yearId")
                                )
                                selectyear1.select_by_value(year1)
                                time.sleep(5)

                                selectmonth1 = Select(
                                    driver.find_element(By.ID, "month")
                                )
                                selectmonth1.select_by_value(month1)
                                driver.implicitly_wait(2)

                                # the second year and month

                                selectyear2 = Select(
                                    driver.find_element(By.ID, "yearIdd")
                                )
                                selectyear2.select_by_value(year2)
                                driver.implicitly_wait(2)

                                selectmonth2 = Select(
                                    driver.find_element(By.ID, "toMonth")
                                )
                                selectmonth2.select_by_value(month2)
                                driver.implicitly_wait(2)

                                # clicking submit

                                driver.find_element(
                                    By.XPATH,
                                    '//*[@id="panelfilter"]/ul/li[4]/div/input[1]',
                                ).click()

                                driver.implicitly_wait(2)

                                driver.execute_script(st_href)

                                driver.implicitly_wait(2)

                                no_of_pages = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )

                                no_of_pages.select_by_value("100")

                                driver.implicitly_wait(2)

                                driver.execute_script(di_href)

                                driver.implicitly_wait(2)

                                no_of_pages_1 = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )

                                no_of_pages_1.select_by_value("100")

                                driver.implicitly_wait(2)

                                driver.execute_script(bl_href)

                                driver.implicitly_wait(2)

                        # sub-sub-except block ends

                    # sub-try block ends

                    # sub-except block begins

                    except NoSuchElementException:

                        print(
                            f"No Such Element raised at {state} {district} {block} {gp}"
                        )

                        driver.get(url)

                        driver.implicitly_wait(2)

                        # the first year and month

                        selectyear1 = Select(
                            driver.find_element(By.ID, "yearId")
                        )
                        selectyear1.select_by_value(year1)
                        time.sleep(5)

                        selectmonth1 = Select(
                            driver.find_element(By.ID, "month")
                        )
                        selectmonth1.select_by_value(month1)
                        driver.implicitly_wait(2)

                        # the second year and month

                        selectyear2 = Select(
                            driver.find_element(By.ID, "yearIdd")
                        )
                        selectyear2.select_by_value(year2)
                        driver.implicitly_wait(2)

                        selectmonth2 = Select(
                            driver.find_element(By.ID, "toMonth")
                        )
                        selectmonth2.select_by_value(month2)
                        driver.implicitly_wait(2)

                        # clicking submit

                        driver.find_element(
                            By.XPATH,
                            '//*[@id="panelfilter"]/ul/li[4]/div/input[1]',
                        ).click()

                        driver.implicitly_wait(2)

                        driver.execute_script(st_href)

                        driver.implicitly_wait(2)

                        no_of_pages = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )

                        no_of_pages.select_by_value("100")

                        driver.implicitly_wait(2)

                        driver.execute_script(di_href)

                        driver.implicitly_wait(2)

                    # sub-except block ends

                # making the district json file

                with open(str(district_json_path), "w") as nested_out_file:

                    json.dump(json_list, nested_out_file, ensure_ascii=False)

            else:
                print(
                    district_json_path,
                    "exists and skipped. Moving to next district.",
                )
                continue

        # try block ends

        # except block 1 begins
        except (
            NoSuchElementException,
            TimeoutException,
            UnicodeEncodeError,
        ) as ex:

            while True:

                # sub-try block begins
                try:

                    print(f"{ex} raised at {state} {district} {block} {gp}")

                    driver.get(url)

                    driver.implicitly_wait(2)

                    # the first year and month

                    selectyear1 = Select(driver.find_element(By.ID, "yearId"))
                    selectyear1.select_by_value(year1)
                    time.sleep(5)

                    selectmonth1 = Select(driver.find_element(By.ID, "month"))
                    selectmonth1.select_by_value(month1)
                    driver.implicitly_wait(2)

                    # the second year and month

                    selectyear2 = Select(driver.find_element(By.ID, "yearIdd"))
                    selectyear2.select_by_value(year2)
                    driver.implicitly_wait(2)

                    selectmonth2 = Select(
                        driver.find_element(By.ID, "toMonth")
                    )
                    selectmonth2.select_by_value(month2)
                    driver.implicitly_wait(2)

                    # clicking submit

                    driver.find_element(
                        By.XPATH,
                        '//*[@id="panelfilter"]/ul/li[4]/div/input[1]',
                    ).click()
                    driver.implicitly_wait(2)

                    driver.execute_script(st_href)

                    driver.implicitly_wait(2)

                    break
                # sub-try block ends

                # sub-except block begins
                except (NoSuchElementException, TimeoutException):

                    continue
                # sub-except block ends

        # except block 1 ends

        # except block 2 begins
        except WebDriverException as ex:

            while True:

                # sub-try block begins
                try:
                    print(f"{ex} raised at {state} {district} {block} {gp}")

                    driver.close()

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

                    # fetching url

                    url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDetail"
                    driver.get(url)
                    driver.implicitly_wait(2)

                    # #the first year and month

                    selectyear1 = Select(driver.find_element(By.ID, "yearId"))
                    selectyear1.select_by_value(year1)
                    time.sleep(5)

                    selectmonth1 = Select(driver.find_element(By.ID, "month"))
                    selectmonth1.select_by_value(month1)
                    driver.implicitly_wait(2)

                    # the second year and month

                    selectyear2 = Select(driver.find_element(By.ID, "yearIdd"))
                    selectyear2.select_by_value(year2)
                    driver.implicitly_wait(2)

                    selectmonth2 = Select(
                        driver.find_element(By.ID, "toMonth")
                    )
                    selectmonth2.select_by_value(month2)
                    driver.implicitly_wait(2)

                    # clicking submit

                    driver.find_element(
                        By.XPATH,
                        '//*[@id="panelfilter"]/ul/li[4]/div/input[1]',
                    ).click()

                    driver.implicitly_wait(2)

                    driver.execute_script(st_href)

                    driver.implicitly_wait(2)

                    break
                # sub-try block ends

                # sub-except block begins
                except (NoSuchElementException, TimeoutException):
                    continue
                # sub-except block ends

        # scraping over

print("Looping has ended. Scraper rests.")

# closing browser

driver.close()