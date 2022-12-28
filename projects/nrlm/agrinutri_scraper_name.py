import json
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

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
url = "https://nrlm.gov.in/AgriNutiGardenAction.do?methodName=showView"

master_list = (
    []
)  # Final list of all dictionaries containing names throughout all levels of data
total_count = [0, 0, 0, 0, 0]  # Counter used to iterate through data
district_list = []  # list containing list of district-block-gp-village information
#  Scraping Process Begins
while True:
    try:
        driver.get(url)
        driver.implicitly_wait(5)

        states = driver.find_elements(By.XPATH, "//table[@id='example']/tbody/tr/td[2]")
        # Adding state names to list
        state_name = []
        for i in range(0, len(states)):
            state_name.append(states[i].text)
        state_buttons = driver.find_elements(
            By.XPATH, "//button[@class='idEditButton']"
        )
        number_of_states = len(state_buttons)
        curr_state = state_buttons[total_count[0]].text

        # State to District
        if total_count[0] < number_of_states:
            state_buttons[total_count[0]].click()
            district_buttons = driver.find_elements(
                By.XPATH, '//*[@id="btnwrap"]/button'
            )
            number_of_dist = len(district_buttons)
            curr_dist = district_buttons[total_count[1]].text

            state_path = Path.joinpath(raw_path, curr_state)
            json_name = ".".join([curr_dist, "json"])
            district_json_path = Path.joinpath(raw_path, curr_state, json_name)

            if not state_path.exists():
                Path.mkdir(state_path, parents=True)

            # District to Block
            if district_json_path.exists():
                total_count[1] += 1
                total_count[2] = 0
                total_count[3] = 0
                total_count[4] = 0
            if (total_count[1] < number_of_dist) and (not district_json_path.exists()):
                district_buttons[total_count[1]].click()
                driver.implicitly_wait(10)

                block_buttons = driver.find_elements(
                    By.XPATH, '//*[@id="btnwrap"]/button'
                )
                number_of_blocks = len(block_buttons)
                curr_block = block_buttons[total_count[2]].text

                # Block to Gram Panchayat
                if total_count[2] < number_of_blocks:
                    block_buttons[total_count[2]].click()
                    driver.implicitly_wait(10)
                    gp_buttons = driver.find_elements(
                        By.XPATH, '//*[@id="btnwrap"]/button'
                    )
                    number_of_gp = len(gp_buttons)
                    curr_gp = gp_buttons[total_count[3]].text

                    # Gram Panchayat to Village
                    if total_count[3] < number_of_gp:
                        gp_buttons[total_count[3]].click()
                        driver.implicitly_wait(10)
                        village_list = driver.find_elements(
                            By.XPATH, '//*[@id="example"]/tbody[1]/tr/td[2]'
                        )
                        number_of_villages = len(village_list)
                        # Adding State-Dist-Block-GP-Village Level Names to a master_list
                        if number_of_villages > 0:
                            for i in range(0, number_of_villages):
                                val = {}
                                curr_village = village_list[i].text
                                val[
                                    "state_name"
                                ] = curr_state  # add the placeholder value for empty string values later
                                val["district_name"] = curr_dist
                                val["block_name"] = curr_block
                                val["gram_panchayat_name"] = curr_gp
                                val["village_name"] = curr_village
                                master_list.append(val)
                                district_list.append(val)
                                # print(val)
                                # print(master_list)
                                print("Counter List:", total_count)
                                total_count[4] += 1
                        else:
                            val = {}
                            val["state_name"] = curr_state
                            val["district_name"] = curr_dist
                            val["block_name"] = curr_block
                            val["gram_panchayat_name"] = curr_gp
                            val["village_name"] = "NO-VILLAGE"
                            master_list.append(val)
                            district_list.append(val)
                            # print(val)
                            # print(master_list)
                            print("Counter List:", total_count)
                            total_count[4] += 1
                        total_count[4] = 0
                        back = driver.find_element(
                            By.XPATH, '//*[@id="backButtonId"]/input'
                        )
                        back.click()
                        driver.implicitly_wait(10)
                        total_count[3] += 1

                    if total_count[3] >= number_of_gp:
                        total_count[2] += 1
                        total_count[3] = 0
                if (total_count[2] >= number_of_blocks) and (
                    not district_json_path.exists()
                ):
                    total_count[1] += 1
                    total_count[2] = 0
                    with open(str(district_json_path), "w") as nested_out_file:
                        json.dump(district_list, nested_out_file, ensure_ascii=False)
                    district_list = []
            if total_count[1] >= number_of_dist:
                total_count[0] += 1
                total_count[1] = 0
            state_buttons = driver.find_elements(
                By.XPATH, "//button[@class='idEditButton']"
            )
            driver.implicitly_wait(10)
    except WebDriverException:
        # print(ex)
        continue

    if total_count[0] >= number_of_states:
        break
