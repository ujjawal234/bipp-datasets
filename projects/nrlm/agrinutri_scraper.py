# import json
from pathlib import Path

# import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (  # NoSuchElementException,; TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = Path.joinpath(dir_path, "data", "external")
raw_path = Path.joinpath(dir_path, "data", "raw")

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
url = "https://nrlm.gov.in/AgriNutiGardenAction.do?methodName=showView"

total_count = [0, 0, 0, 0, 0]

# Scraping Process Begins
while True:
    try:
        driver.get(url)
        driver.implicitly_wait(5)

        states = driver.find_elements(
            By.XPATH, "//table[@id='example']/tbody/tr/td[2]"
        )

    except WebDriverException:
        continue
