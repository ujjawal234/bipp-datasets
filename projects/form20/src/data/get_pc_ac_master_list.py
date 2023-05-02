from pathlib import Path
from time import sleep
import numpy as np
import pandas as pd
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager



class master_list:
    """
    Class for scraping and compiling Parliamentary (pc) and Assembly (ac) constituency master list by state
    """

    def __init__(self):
        """Initializes the scraper"""

        # defining directories
        dir_path = Path.cwd()
        self.election_state_json_path=dir_path.joinpath("raw","master_list","jsons","election_state.json")
        self.pc_jsons_path=dir_path.joinpath("raw","master_list","jsons","pc_jsons")
        self.ac_jsons_path=dir_path.joinpath("raw","master_list","jsons","ac_jsons")

        if not self.pc_jsons_path.exists():
            self.pc_jsons_path.mkdir(parents=True)

        if not self.ac_jsons_path.exists():
            self.ac_jsons_path.mkdir(parents=True)


        # defining Chrome options
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": str(dir_path),
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument("--ignore-certificate-errors")

        self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        self.url = "https://www.elections.in/assembly-constituencies.html"

    def election_state_json_create(self):
        """
        
        """

        if self.election_state_json_path.exists():
            print()


    def pc_json_create(self, election:str, state:str, pcs:list):

        """
        This function creates multiple json files with PC details for the PC master list.
        """

        #checking if pc_json exits or not
        pc_path=self.pc_jsons_path.joinpath(f"{election}_{state}.json")

        if not pc_path.exists():

            print(f"PC jsons for {election} {state} doesn't exist. Creating it.")

            pc_dict={
                "election":election,
                "state":state,
                "pc":pcs
            }

            with open(str(pc_path), "w") as pc_json:
                    json.dump(
                        pc_dict, pc_json, ensure_ascii=True
                    )

        else:
            print(f"PC jsons for {election} {state} exists. Skipping it.")

            
    def ac_json_create(self, election:str, state:str, pc:str, acs:list):
        """
        This function creates multiple json files with AC details for the AC master list.
        """

        #checking if pc_json exits or not
        ac_folder_path=self.ac_jsons_path.joinpath(f"{election}", f"{state}")

        if not ac_folder_path.exists():
            ac_folder_path.mkdir(parents=True)

        ac_json_path=ac_folder_path.joinpath(f"{pc}.json")

        if not ac_json_path.exists():

            print(f"AC jsons for {election} {state} {pc} doesn't exist. Creating it.")

            ac_dict={
                "election":election,
                "state":state,
                "pc":pc,
                "ac":acs
            }

            with open(str(ac_json_path), "w") as ac_json:
                    json.dump(
                        ac_dict, ac_json, ensure_ascii=True
                    )

        else:
            print(f"AC jsons for {election} {state} {pc} exists. Skipping it.")


    def scraper(self):

        """
        This function calls the driver and calls the necessary functions in the class to construct the master list.
        """

        driver=self.driver

        driver.get(self.url)
        sleep(2)    

        
        #loading the election drop down list
        election_dropdown=Select(driver.find_element(By.XPATH, './/select[@id="type"]'))
        elections=[x.get_attribute("text") for x in election_dropdown.options]
        sleep(2)

        for election in elections:
            election_dropdown.select_by_value(election)
            sleep(2)

            #loading the state dropdown list
            state_dropdwon=Select(driver.find_element(By.XPATH, './/select[@id="state"]'))
            states=[x.get_attribute("text") for x in state_dropdwon.options]
            sleep(2)

            for state in states:
                state_dropdwon.select_by_value(state)
                sleep(2)

                #loading pc dropdown list
                pc_dropdown=Select(driver.find_element(By.XPATH, './/select[@id="constituency"]'))
                pc_names=[x.get_attribute("text") for x in pc_dropdown.options]

                #calling in the pc_json_create to make the pc jsons for each state
                master_list.pc_json_create(election=election, state=state, pcs=pc_names)

                for pc in pc_names:

                    pc_dropdown.select_by_value(pc)
                    sleep(2)

                    submit=driver.find_element(By.XPATH, './/input[@class="subbutton"]')
                    submit.click()
                    sleep(2)

                    #extracting the table with AC names in the PC page
                    ac_table=driver.find_elements(By.XPATH, './/div[@class="mob-table"]//table//tr//td//a')
                    ac_names=[x.get_attribute("text") for x in ac_table]

                    #calling in the ac_json_create to make the ac jsons for each PC
                    master_list.ac_json_create(election=election, state=state, pc=pc, acs=ac_names)



def main():
    scrape = master_list()


if __name__ == "__main__":
    main()
