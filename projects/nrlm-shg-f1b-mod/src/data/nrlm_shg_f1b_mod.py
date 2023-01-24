import json
import re
from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException, TimeoutException, WebDriverException,)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd

class Downloader:
    """
    This class is used to download data from a website. It takes in one parameter, time_stamp,
    which is used to create the file directory paths. It uses Selenium webdriver to automate
    the downloading process.
    """
    def __init__(self, time_stamp):
        """
        Initialize the class with the following parameters:
        :param time_stamp: timestamp used to create the file directory paths
        """
        self.time_stamp = time_stamp
        self.dir_path = Path.cwd()
        self.raw_path = Path.joinpath(self.dir_path, "data", "raw", self.time_stamp)
        self.interim_path = Path.joinpath(self.dir_path, "data", "interim", self.time_stamp)
        self.all_names_path = Path.joinpath(self.interim_path, "all_names.json")
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("start-maximized")
        self.chrome_options.add_argument("--ignore-certificate-errors")
        self.prefs = {"download.default_directory": str(self.dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
        self.chrome_options.add_experimental_option("prefs", self.prefs)
        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=self.chrome_options)

    def fetch_data(self, url, year1, month1, year2, month2):
        """
        This function navigates to the specified url and selects the specified year and month values from the dropdown menus on the webpage.
        It then clicks on the submit button on the webpage.
        
        Parameters:
        - self: object of the class
        - url: str, the url to navigate to
        - year1: str, value to select for the first year dropdown menu
        - month1: str, value to select for the first month dropdown menu
        - year2: str, value to select for the second year dropdown menu
        - month2: str, value to select for the second month dropdown menu
        """
        with open(str(self.all_names_path), "r") as infile:
            all_names = json.load(infile)
        self.driver.get(url)
        self.driver.implicitly_wait(5)

        selectyear1 = Select(self.driver.find_element(By.ID, 'yearId'))
        selectyear1.select_by_value(year1)
        time.sleep(15)

        selectmonth1 = Select(self.driver.find_element(By.ID, 'month'))
        selectmonth1.select_by_value(month1)
        self.driver.implicitly_wait(5)

        selectyear2 = Select(self.driver.find_element(By.ID, 'yearIdd'))
        selectyear2.select_by_value(year2)
        self.driver.implicitly_wait(5)

        selectmonth2 = Select(self.driver.find_element(By.ID, 'toMonth'))
        selectmonth2.select_by_value(month2)
        self.driver.implicitly_wait(5)

        self.driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
        self.driver.implicitly_wait(5)

    def scrape_main_table(year1, month1):
        main_table_element = driver.find_element(By.XPATH, '//table[@id="tablee"]')
        main_table_element_html = main_table_element.get_attribute("outerHTML")
        main_table= pd.read_html(main_table_element_html)
        main_table=main_table[0]

        #changing multiindex to single index
        main_table.columns = main_table.columns.droplevel(2)
        main_table.columns = main_table.columns.map('_'.join).str.strip('_')
        main_table.columns = [x.lower() for x in main_table.columns]
        main_table.columns = [x.replace(" ", "_").replace(".", "").replace("(rs)","_rs_in_lakh") for x in main_table.columns]
        main_table.rename(columns = {'sr_no_sr_no':'sr_no', 'state_name_state_name':'state_name'}, inplace = True)

        #removing unnecessary rows
        main_table = main_table[pd.to_numeric(main_table["sr_no"], errors='coerce').notnull()]

        #dropping first column
        main_table.drop(columns=main_table.columns[0], axis=1, inplace=True)

        #adding columns for year, month
        main_table.insert(0,'year', year1)
        main_table.insert(1,'month', month1)

        return main_table



    def scrape_state_data(state_name):
        # selecting the state href web element
        state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
        state_names = [x.get_attribute("text") for x in state_page]
        state_hrefs = [x.get_attribute("href") for x in state_page]

        state_dict = {}

        for state, st_href in zip(state_names, state_hrefs):
            if state == state_name:
                state_dict = {state: st_href}
        driver.execute_script(state_dict[state_name])
        driver.implicitly_wait(5)

        #selecting option to view 100 pages
        no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
        no_of_pages.select_by_value("100")

        driver.implicitly_wait(5)

        #retrieving the state table
        state_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
        state_table_element_html = state_table_element.get_attribute("outerHTML")
        state_table= pd.read_html(state_table_element_html)
        state_table=state_table[0]

        driver.implicitly_wait(5)

        while True:
            NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
            next_page_class = NextButton.get_attribute("class")
            if next_page_class != "paginate_button next disabled":
                NextButton.click()
                print("NEXT Button exists for " + state + ". Scraping district names the next page." )

                driver.implicitly_wait(5)

                state_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                state_table_element_html_new = state_table_element_new.get_attribute("outerHTML")
                state_table_new= pd.read_html(state_table_element_html_new)
                state_table_new=state_table_new[0]
                state_table.append(state_table_new)

                driver.implicitly_wait(5)

            else:
                break

        #changing multiindex to single index
        state_table.columns = state_table.columns.droplevel(2)
        state_table.columns = state_table.columns.map('_'.join).str.strip('_')
        state_table.columns = [x.lower() for x in state_table.columns]
        state_table.columns = [x.replace(" ", "_").replace(".", "").replace("(rs)","_rs_in_lakh") for x in state_table.columns]
        state_table.rename(columns = {'sr_no_sr_no':'sr_no', 'district_name<_district_name<':'district_name'}, inplace = True)
        return state_table


if __name__ == '__main__':
    
    url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDetail"
    #downloader.fetch_data(url, '2022', '07', '2022', '07')

    downloader = Downloader("2022_July")
    downloader.fetch_data(url, '2022', '07', '2022', '07')
    main_table = downloader.scrape_main_table('2022', '07')
    all_states = pd.read_csv(str(downloader.all_states_file_path))
    for index, row in all_states.iterrows():
        state_name = row["state_name"]
        state_data = downloader.scrape_state_data(state_name)
        # Save the state data to a CSV file in the corresponding folder
        state_folder_path = Path.joinpath(downloader.raw_path, state_name.lower().strip().replace(" ", "_"))
        state_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", state_name.lower().strip().replace(" ", "_"))
        state_file_path = Path.joinpath(state_folder_path, f"{state_name_corrected}.csv")
        if not state_folder_path.exists():
            state_folder_path.mkdir(parents=True)
        state_data.to_csv(state_file_path, index=False)

        # Close driver
        driver.close()