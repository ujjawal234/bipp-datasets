import json
import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# import bs4 as bs


# defining directories
time_stamp = "2022_23_April"

dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp, "jsons")
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = Path.joinpath(dir_path, "data", "external")


"""Scraping Begins Here"""


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


# fectching URL
url = "https://nrlm.gov.in/CommunityInvestmentFundDisbursementAction.do?methodName=showDetail"
driver.get(url)

driver.implicitly_wait(5)
while True:
    try:

        driver.get(url)

        driver.implicitly_wait(10)

        # ************************POINT OF MANUAL ITERATION********************#
        # selecting the year web element
        year_select = Select(driver.find_element(By.NAME, "year"))
        year = "2022"
        year_select.select_by_value(year)
        time.sleep(5)
        year_select = Select(driver.find_element(By.NAME, "toYear"))
        year = "2022"
        year_select.select_by_value(year)

        time.sleep(5)

        # selecting the month web element
        month_select = Select(driver.find_element(By.NAME, "month"))
        month = "04"  # April
        month_select.select_by_value(month)

        time.sleep(5)
        month_select = Select(driver.find_element(By.NAME, "toMonth"))
        month = "04"  # April
        month_select.select_by_value(month)

        time.sleep(5)

        # clicking submit button
        SubmitButton = driver.find_element(
            By.XPATH, "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]"
        )
        SubmitButton.click()

        # *********************************************************************#

        driver.implicitly_wait(10)

        state_page = driver.find_elements(
            By.XPATH, "//*[@class='panel panel-default']//a"
        )
        state_page = state_page[2:]

        state_names = [x.get_attribute("text") for x in state_page]
        state_hrefs = [x.get_attribute("href") for x in state_page]

        for state, st_href in zip(state_names, state_hrefs):
            driver.execute_script(st_href)
            driver.implicitly_wait(10)
            district_row_select = Select(
                driver.find_element(
                    By.XPATH, '//*[@id="example_length"]/label/select'
                )
            )
            district_row_select.select_by_value("100")
            district_table = driver.find_elements(
                By.XPATH, "//*[@id='example']//a"
            )
            district_names = [x.get_attribute("text") for x in district_table]
            district_hrefs = [x.get_attribute("href") for x in district_table]
            driver.implicitly_wait(10)

            for district, dist_href in zip(district_names, district_hrefs):

                district_list = []
                state_path = Path.joinpath(raw_path, state)
                json_name = ".".join([district, "json"])
                district_json_path = Path.joinpath(raw_path, state, json_name)
                if not state_path.exists():
                    Path.mkdir(state_path, parents=True)

                if not district_json_path.exists():
                    print(
                        district_json_path,
                        "doesn't exist and proceeding for scraping",
                    )
                    district_row_select = Select(
                        driver.find_element(
                            By.XPATH, '//*[@id="example_length"]/label/select'
                        )
                    )

                    district_row_select.select_by_value("100")

                    driver.execute_script(dist_href)

                    driver.implicitly_wait(10)

                    block_row_select = Select(
                        driver.find_element(
                            By.XPATH, '//*[@id="example_length"]/label/select'
                        )
                    )

                    block_row_select.select_by_value("100")

                    driver.implicitly_wait(10)

                    block_table = driver.find_elements(
                        By.XPATH, "//*[@id='example']//a"
                    )

                    block_names = [
                        x.get_attribute("text") for x in block_table
                    ]

                    block_hrefs = [
                        x.get_attribute("href") for x in block_table
                    ]
                    print(block_names[0])

                    while True:

                        NextButton = driver.find_element(
                            By.XPATH, '//*[@id="example_next"]'
                        )
                        next_page_class = NextButton.get_attribute("class")

                        if next_page_class != "paginate_button next disabled":

                            NextButton.click()

                            print(
                                f"NEXT Button exists for {state} {district}. Scraping block names the next page."
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

                        # driver.implicitly_wait(10)

                        else:
                            break

                    for block, block_href in zip(block_names, block_hrefs):

                        print(
                            f"Attempting scrape of {state} {district} {block}"
                        )

                        block_row_select = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )

                        block_row_select.select_by_value("100")

                        driver.implicitly_wait(10)

                        driver.execute_script(block_href)

                        driver.implicitly_wait(10)

                        gp_row_select = Select(
                            driver.find_element(
                                By.XPATH,
                                '//*[@id="example_length"]/label/select',
                            )
                        )

                        gp_row_select.select_by_value("100")

                        driver.implicitly_wait(10)

                        gp_table = driver.find_elements(
                            By.XPATH, "//*[@id='example']//a"
                        )

                        gp_names = [x.get_attribute("text") for x in gp_table]
                        gp_hrefs = [x.get_attribute("href") for x in gp_table]

                        # ***********************************************************************************************************************************#

                        # """Checking for the NEXT button in GP list of block page"""

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
                                    f"NEXT Button exists for {state} {district} {block}. Scraping GP names the next page."
                                )

                                driver.implicitly_wait(5)

                                gp_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )

                                gp_names_next = [
                                    x.get_attribute("text") for x in gp_table
                                ]
                                gp_href_next = [
                                    x.get_attribute("href") for x in gp_table
                                ]

                                gp_names.extend(gp_names_next)
                                gp_hrefs.extend(gp_href_next)

                                driver.implicitly_wait(10)

                            else:
                                break

                        for gp, gp_href in zip(gp_names, gp_hrefs):

                            try:  # TRY3
                                print(
                                    f"Attempting scrape of {state} {district} {block} {gp}"
                                )
                                gp_row_select = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )

                                gp_row_select.select_by_value("100")

                                driver.implicitly_wait(10)

                                driver.execute_script(gp_href)

                                driver.implicitly_wait(10)

                                village_row_select = Select(
                                    driver.find_element(
                                        By.XPATH,
                                        '//*[@id="example_length"]/label/select',
                                    )
                                )

                                village_row_select.select_by_value("100")

                                driver.implicitly_wait(10)

                                village_table = driver.find_elements(
                                    By.XPATH, "//*[@id='example']//a"
                                )

                                village_names = [
                                    x.get_attribute("text")
                                    for x in village_table
                                ]

                                # ***********************************************************************************************************************************#

                                # """Checking for the NEXT button in GP list of block page"""

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

                                        print(
                                            f"NEXT Button exists for {state} {district} {block}. Scraping VILLAGE names the next page."
                                        )

                                        driver.implicitly_wait(5)

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

                                        driver.implicitly_wait(10)

                                    else:
                                        break

                                for village in village_names:
                                    name_split = [i for i in village]

                                    block_dict = {}

                                    block_dict = {
                                        "state_name": state,
                                        "district_name": district,
                                        "block_name": block,
                                        "gp_name": gp,
                                        "village_name": village,
                                    }

                                    district_list.append(block_dict)

                            except NoSuchElementException:  # except for try 3
                                print(
                                    f"No Such Element raised at {state} {district} {block} {gp}"
                                )
                                # going back to block

                        BackButton = driver.find_element(
                            By.XPATH,
                            '//*[@id="panelfilter"]/ul/li[4]/div/input[2]',
                        ).click()

                        BackButton = driver.find_element(
                            By.XPATH,
                            '//*[@id="panelfilter"]/ul/li[4]/div/input[2]',
                        ).click()
                    with open(str(district_json_path), "w") as nested_out_file:
                        json.dump(
                            district_list, nested_out_file, ensure_ascii=False
                        )
                    BackButton = driver.find_element(
                        By.XPATH,
                        '//*[@id="panelfilter"]/ul/li[4]/div/input[2]',
                    ).click()
                else:
                    print(
                        district_json_path,
                        "exists and skipped. Moving to next district.",
                    )
                    continue
        BackButton = driver.find_element(
            By.XPATH,
            '//*[@id="panelfilter"]/ul/li[4]/div/input[2]',
        ).click()

        break

    except (NoSuchElementException, TimeoutException) as ex:
        print(ex, "has been raised")
