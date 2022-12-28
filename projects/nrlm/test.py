import time
from pathlib import Path

from selenium import webdriver
from selenium.common.exceptions import WebDriverException
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
state_count = 0  # Counts at state-level
master_list = []
dist_count = {}  # Counts at district level for each state
while True:
    try:
        driver.get(url)
        time.sleep(3)
        # State to District Scraping

        states = driver.find_elements(
            By.XPATH, "//table[@id='example']/tbody/tr/td[2]"
        )
        # Adding state names to list (why? can be used to callback later?)
        state_name = []
        for i in range(0, len(states)):
            state_name.append(states[i].text)
            state_buttons = driver.find_elements(
                By.XPATH, "//button[@class='idEditButton']"
            )
        number_of_states = len(state_buttons)
        curr_state = state_buttons[state_count].text
        if state_count < number_of_states:
            if curr_state not in dist_count:
                dist_count[curr_state] = 0
            state_buttons[state_count].click()
            district_name = []
            district_buttons = driver.find_elements(
                By.XPATH, '//*[@id="btnwrap"]/button'
            )
            number_of_dist = len(district_buttons)
            # print(district_buttons)

            curr_dist = district_buttons[dist_count[curr_state]].text
            if dist_count[curr_state] < number_of_dist:
                district_buttons[dist_count[curr_state]].click()
                time.sleep(5)
                # Collecting block names into list
                block_name = []
                block_buttons = driver.find_elements(
                    By.XPATH, '//*[@id="btnwrap"]/button'
                )
                number_of_blocks = len(block_buttons)
                for bl in block_buttons:
                    if len(bl.text) != 0:
                        block_name.append(bl.text)
                    else:
                        block_name.append("FIND BLOCK VALUE")
                print(block_name)
                print(curr_dist)
            back = driver.find_element(
                By.XPATH, '//*[@id="backButtonId"]/input'
            )
            back.click()
            time.sleep(3)
            dist_count[curr_state] += 1
            if dist_count[curr_state] >= number_of_dist:
                state_count += 1

    except WebDriverException:
        driver.close()
        break
