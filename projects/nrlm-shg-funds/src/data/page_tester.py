import json
import re
from pathlib import Path

# import bs4 as bs
# import numpy as np
# import pandas as pd
from selenium import webdriver

# from selenium.common.exceptions import (
#     NoSuchElementException,
#     TimeoutException,
#     WebDriverException,
# )
from selenium.webdriver.common.by import By

# from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

# import lxml


dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", "2021_22_March")
interim_path = Path.joinpath(dir_path, "data", "interim", "2021_22_March")
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

driver = webdriver.Chrome(
    ChromeDriverManager().install(), options=chrome_options
)


# fectching URL
url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDisbursementByCurrentMonthAndYear"
driver.get(url)

driver.implicitly_wait(10)

# ************************POINT OF MANUAL ITERATION********************#
# selecting the year web element
year_select = Select(driver.find_element(By.NAME, "year"))
year = "2021-2022"
year_select.select_by_value(year)

driver.implicitly_wait(5)

# selecting the month web element
month_select = Select(driver.find_element(By.NAME, "month"))
month = "03"
month_select.select_by_value("03")

driver.implicitly_wait(5)

# clicking submit button
SubmitButton = driver.find_element(
    By.XPATH, "/html/body/div[4]/form/div[1]/ul/li[4]/div/input[1]"
)
SubmitButton.click()

driver.implicitly_wait(5)
# *********************************************************************#


driver.execute_script("javascript:getDetail('33','CHHATTISGARH')")

driver.implicitly_wait(20)

driver.execute_script("javascript:getDetail('3303','DURG')")

driver.implicitly_wait(10)

driver.execute_script("javascript:getDetail('3303007','PATAN')")

driver.implicitly_wait(10)


gp_row_select = Select(
    driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select')
)

gp_row_select.select_by_value("100")

driver.implicitly_wait(10)


gp_table = driver.find_elements(By.XPATH, "//*[@id='example']//a")

gp_hrefs = [x.get_attribute("href") for x in gp_table]
gp_names = [x.get_attribute("text") for x in gp_table]


# while True:

#     NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
#     next_page_class = NextButton.get_attribute("class")

#     print(next_page_class)

#     if next_page_class != "paginate_button next disabled":

#         NextButton.click()

#         print("STEP 1")

#         driver.implicitly_wait(5)

#         gp_table=driver.find_elements(By.XPATH, "//*[@id='example']//a")

#         print("Step 2")

#         gp_names_next=[x.get_attribute('text') for x in gp_table]

#         gp_names.extend(gp_names_next)

#         print("Step 3")

#         driver.implicitly_wait(10)


#     else:
#         print("NO Next Button anymore")

#         break

# print(gp_names,gp_hrefs)


for row in all_names:
    if (
        row["state_name"] == "CHHATTISGARH"
        and row["district_name"] == "DURG"
        and row["block_name"] == "PATAN"
        and row["gp_name"] == "JAMGAON "
    ):
        row["gp_name"] = re.sub(r"[^A-Za-z0-9_]", "", row["gp_name"])
        print(row["gp_name"])

        row_gp_name = row["gp_name"]

for gp_name, gp_href in zip(gp_names, gp_hrefs):
    gp_name = re.sub(r"[^A-Za-z0-9_]", "", gp_name)
    gp_name = "".join(gp_name.split("\xa0"))

    if gp_name == row_gp_name:
        print(gp_name, gp_href)
        gp = gp_name
