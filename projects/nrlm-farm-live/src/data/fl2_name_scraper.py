import json
import re
from pathlib import Path
from time import sleep

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

# defining directories
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw")
interim_path = Path.joinpath(dir_path, "data", "interim")

# defining a et of counters to filter out sates already scraped in case the webpage is recalled in the time of an exception
last_year_counter = 0
# last_month_counter = 0
# last_state_counter = 0
checker_string = "just some text!"  # checker cstring to place inside the text files which acts as identifiers for already scarped states and months


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
url = "https://nrlm.gov.in/FLMPRIndicatorsWiseAction.do?methodName=showDetail"

# driver.get(url)

while True:
    try:

        driver.get(url)

        sleep(3)

        years_select = Select(
            driver.find_element(By.XPATH, '//*[@id="yearId"]')
        )
        years = [year.get_attribute("value") for year in years_select.options]
        print(years)

        for year in years:
            driver.find_element(By.XPATH, '//*[@id="yearId"]').click()
            years_select = Select(
                driver.find_element(By.XPATH, '//*[@id="yearId"]')
            )
            years_select.select_by_value(year)
            sleep(3)

            # selecting the from and to month
            from_month_select = Select(
                driver.find_element(By.XPATH, '//*[@id="fmonth"]')
            )
            month_names = [
                x.get_attribute("text") for x in from_month_select.options
            ]
            month_values = [
                x.get_attribute("value") for x in from_month_select.options
            ]
            # print(month_names)
            # print(month_values)
            sleep(3)

            for month_name, month_value in zip(month_names, month_values):

                month_check_path = raw_path.joinpath(
                    "jsons", year, f"{year}_{month_name}.txt"
                )
                if not month_check_path.exists():
                    # selcting from month
                    driver.find_element(By.XPATH, '//*[@id="fmonth"]').click()
                    from_month_select = Select(
                        driver.find_element(By.XPATH, '//*[@id="fmonth"]')
                    )
                    from_month_select.select_by_value(month_value)
                    sleep(3)

                    # selecting to month
                    driver.find_element(By.XPATH, '//*[@id="tmonth"]').click()
                    to_month_select = Select(
                        driver.find_element(By.XPATH, '//*[@id="tmonth"]')
                    )
                    to_month_select.select_by_value(month_value)
                    sleep(3)

                    state_select = Select(
                        driver.find_element(By.XPATH, '//*[@id="stateId"]')
                    )
                    state_names = [
                        state_name.get_attribute("text")
                        for state_name in state_select.options
                    ][1:]
                    state_codes = [
                        state_code.get_attribute("value")
                        for state_code in state_select.options
                    ][1:]

                    for state_name, state_code in zip(
                        state_names, state_codes
                    ):

                        state_check_path = raw_path.joinpath(
                            "jsons", year, month_name, f"{state_name}.txt"
                        )

                        if not state_check_path.exists():

                            state_select.select_by_value(state_code)
                            sleep(3)

                            district_select = Select(
                                driver.find_element(
                                    By.XPATH, '//*[@id="districtId"]'
                                )
                            )
                            district_names = [
                                district_name.get_attribute("text")
                                for district_name in district_select.options
                            ][1:]
                            district_codes = [
                                district_code.get_attribute("value")
                                for district_code in district_select.options
                            ][1:]

                            for district_name, district_code in zip(
                                district_names, district_codes
                            ):

                                # cleaning state and district names to make folder structures

                                state = re.sub(
                                    r"[^A-Za-z0-9_]",
                                    "",
                                    state_name.strip().replace(" ", "_"),
                                )

                                district = re.sub(
                                    r"[^A-Za-z0-9_]",
                                    "",
                                    district_name.strip().replace(" ", "_"),
                                )

                                district_file = Path.joinpath(
                                    raw_path,
                                    "jsons",
                                    year,
                                    month_name,
                                    state,
                                    f"{district}.json",
                                )
                                district_path = Path.joinpath(
                                    raw_path, "jsons", year, month_name, state
                                )

                                if not district_path.exists():
                                    Path.mkdir(district_path, parents=True)

                                if state_code == district_code[0:2]:

                                    # checking if the district file exists
                                    if not district_file.exists():

                                        print(
                                            f"{district_file} doesn't exist.Proceeding for scraping"
                                        )

                                        district_list = []

                                        driver.find_element(
                                            By.XPATH, '//*[@id="districtId"]'
                                        ).click()

                                        district_select = Select(
                                            driver.find_element(
                                                By.XPATH,
                                                '//*[@id="districtId"]',
                                            )
                                        )

                                        district_select.select_by_value(
                                            district_code
                                        )

                                        sleep(3)

                                        driver.find_element(
                                            By.XPATH, '//*[@id="blockId"]'
                                        ).click()

                                        block_select = Select(
                                            driver.find_element(
                                                By.XPATH, '//*[@id="blockId"]'
                                            )
                                        )
                                        block_names = [
                                            block_name.get_attribute("text")
                                            for block_name in block_select.options
                                        ][1:]
                                        block_codes = [
                                            block_code.get_attribute("value")
                                            for block_code in block_select.options
                                        ][1:]

                                        for block_name, block_code in zip(
                                            block_names, block_codes
                                        ):

                                            if (
                                                district_code
                                                == block_code[0:4]
                                            ):

                                                block = re.sub(
                                                    r"[^A-Za-z0-9_]",
                                                    "",
                                                    block_name.strip().replace(
                                                        " ", "_"
                                                    ),
                                                )

                                                block_dict = {
                                                    "year": year,
                                                    "month_name": month_name,
                                                    "month_value": month_value,
                                                    "state_name": state,
                                                    "state_code": state_code,
                                                    "district_name": district,
                                                    "district_code": district_code,
                                                    "block_name": block,
                                                    "block_code": block_code,
                                                }

                                                print(state, district, block)

                                                print(block_dict)

                                                district_list.append(
                                                    block_dict
                                                )

                                            else:
                                                print("Code match Error")

                                                print(
                                                    f"{district_name}:{district_code} and {block_name}:{block_code} doesn't match. Getting page again"
                                                )

                                                raise (KeyError)

                                        with open(
                                            str(district_file), "w"
                                        ) as nested_out_file:
                                            json.dump(
                                                district_list,
                                                nested_out_file,
                                                ensure_ascii=False,
                                            )

                                    else:
                                        print(
                                            f"{district_file} exists.Moving to next district"
                                        )
                                        continue

                                else:
                                    print("Code match Error")

                                    print(
                                        f"{state_name}:{state_code} and {district_name}:{district_code} doesn't match. Getting page again"
                                    )

                                    raise (KeyError)

                            with open(
                                str(state_check_path), "w"
                            ) as state_check:
                                json.dump(
                                    checker_string,
                                    state_check,
                                    ensure_ascii=False,
                                )

                        else:
                            print(
                                f"{state_name} has already ben scraped. Moving to new state"
                            )

                    with open(str(month_check_path), "w") as month_check:
                        json.dump(
                            checker_string, month_check, ensure_ascii=False
                        )

                else:
                    print(
                        f"{year} {month_name} has already ben scraped. Moving to next month"
                    )

            last_year_counter += 1

        break

    except (
        NoSuchElementException,
        TimeoutException,
        StaleElementReferenceException,
        ElementNotSelectableException,
    ) as ex:
        print(f"{ex} has been raised")

    except WebDriverException as ex:
        print(f"{ex} has been raised")

    except KeyError:
        print("Catching KeyError. Getting page again.")

print(f"Scraping has ended for year {year} and closing driver. Scraper rests.")
driver.close()
