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

#defining directories

time_stamp="2022_July"
dir_path = Path.cwd()
raw_path = Path.joinpath(dir_path, "data", "raw", time_stamp)
interim_path = Path.joinpath(dir_path, "data", "interim", time_stamp)
all_names_path = Path.joinpath(interim_path, "all_names.json")

#calling in the flat list

with open(str(all_names_path), "r") as infile:
    all_names = json.load(infile)

#defining Chrome options

chrome_options = webdriver.ChromeOptions()
prefs = {"download.default_directory": str(dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--ignore-certificate-errors")
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

#fetching url

url = "https://nrlm.gov.in/RevolvingFundDisbursementAction.do?methodName=showDetail"
driver.get(url)
driver.implicitly_wait(5)

#the first year and month

selectyear1 = Select(driver.find_element(By.ID, 'yearId'))
year1='2022' 
selectyear1.select_by_value(year1)
time.sleep(15)

selectmonth1 = Select(driver.find_element(By.ID, 'month'))
month1='07' 
selectmonth1.select_by_value(month1)
driver.implicitly_wait(5)

#the second year and month

selectyear2 = Select(driver.find_element(By.ID, 'yearIdd'))
year2='2022' 
selectyear2.select_by_value(year2)
driver.implicitly_wait(5)

selectmonth2 = Select(driver.find_element(By.ID, 'toMonth')) 
month2='07'
selectmonth2.select_by_value(month2)
driver.implicitly_wait(5)

#clicking submit

driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
driver.implicitly_wait(5)

#scraping the main table

all_states_file_path = Path.joinpath(raw_path, "all_states.csv")
if not all_states_file_path.exists():
    print ("Scraping all states table...")
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

    #storing all states table as csv
    if not raw_path.exists():
        Path.mkdir(raw_path, parents=True)
    main_table.to_csv(Path.joinpath(raw_path, "all_states.csv"), index=False)
    print ("Scrapped all states table. Proceeding to state level scraping...")

else:
    print ("The csv for all states already exists. Proceeding to state level scraping...")

#checking how many rows there have to be scraped

total_rows=len(all_names)
count_variable=0

#looping across all names

for row in all_names:

    count_variable+=1

    print ("Scraping "+str(count_variable)+"/"+str(total_rows))

    #scraping state

    state_folder_path = Path.joinpath(raw_path, row["state_name"].lower().strip().replace(" ", "_"))
    state_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["state_name"].lower().strip().replace(" ", "_"))
    state_file_path = Path.joinpath(state_folder_path, f"{state_name_corrected}.csv")

    if not state_folder_path.exists():
        state_folder_path.mkdir(parents=True)

    if not state_file_path.exists():
        print("csv for state "+state_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1

                # selecting the state href web element
                state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):

                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                driver.execute_script(state_dict[row["state_name"]])
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
                state_table.drop('unnamed:_14_level_0_unnamed:_14_level_1', inplace=True, axis=1)
                state_table.drop('sr_no', inplace=True, axis=1)
                state_table.drop(state_table.index[-1], inplace=True, axis=0)

                #adding columns for year, month
                state_table.insert(0,'year', year1)
                state_table.insert(1,'month', month1)
                state_table.insert(2,'state_name', row['state_name'])

                #storing state table as csv

                state_table.to_csv(Path.joinpath(state_folder_path, f"{state_name_corrected}.csv"), index=False)

                print ("Scraped " + state + " table. Moving to next scraping task.")

            
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} ")

                driver.close()

                print("Calling driver again")

                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path), "profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
        print("csv for state "+state_name_corrected+ " exists. Moving to next scraping task.")

    #scraping district

    district_folder_path = Path.joinpath(state_folder_path, row["district_name"].lower().strip().replace(" ", "_"))
    district_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["district_name"].lower().strip().replace(" ", "_"))
    district_file_path = Path.joinpath(district_folder_path, f"{district_name_corrected}.csv")

    if not district_folder_path.exists():
        district_folder_path.mkdir(parents=True)

    if not district_file_path.exists():
        print("csv for district "+district_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1

                driver.get(url)

                driver.implicitly_wait(5)

                #the first year and month

                selectyear1 = Select(driver.find_element(By.ID, 'yearId'))
                year1='2022' 
                selectyear1.select_by_value(year1)
                time.sleep(15)

                selectmonth1 = Select(driver.find_element(By.ID, 'month'))
                month1='07' 
                selectmonth1.select_by_value(month1)
                driver.implicitly_wait(5)

                #the second year and month

                selectyear2 = Select(driver.find_element(By.ID, 'yearIdd'))
                year2='2022' 
                selectyear2.select_by_value(year2)
                driver.implicitly_wait(5)

                selectmonth2 = Select(driver.find_element(By.ID, 'toMonth')) 
                month2='07'
                selectmonth2.select_by_value(month2)
                driver.implicitly_wait(5)

                #clicking submit

                driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
                driver.implicitly_wait(5)

                #selecting the state href web element
                state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):
                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)
                            
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + state + ". Scraping district names the next page." )

                        driver.implicitly_wait(5)

                        district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        district_names_next = [x.get_attribute("text") for x in district_page]
                        district_names.extend(district_names_next)
                        district_hrefs_next = [x.get_attribute("href") for x in district_page]
                        district_hrefs.extend(district_hrefs_next)

                    else:

                        break
                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}
                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(5)


                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                #retrieving the district table
                district_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                district_table_element_html = district_table_element.get_attribute("outerHTML")
                district_table= pd.read_html(district_table_element_html)
                district_table= district_table[0]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        district_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        district_table_element_html_new = district_table_element_new.get_attribute("outerHTML")
                        district_table_new= pd.read_html(district_table_element_html_new)
                        district_table_new=district_table_new[0]
                        district_table.append(district_table_new)

                        driver.implicitly_wait(5)

                    else:

                        break                                                               


                #changing multiindex to single index

                district_table.columns = district_table.columns.droplevel(2)
                district_table.columns = district_table.columns.map('_'.join).str.strip('_')
                district_table.columns = [x.lower() for x in district_table.columns]
                district_table.columns = [x.replace(" ", "_").replace(".", "").replace("(rs)","_rs") for x in district_table.columns]
                district_table.rename(columns = {'sr_no_sr_no':'sr_no', 'block_name_block_name':'block_name'}, inplace = True)
                district_table.drop('unnamed:_14_level_0_unnamed:_14_level_1', inplace=True, axis=1)
                district_table.drop('sr_no', inplace=True, axis=1)
                district_table.drop(district_table.index[-1], inplace=True, axis=0)

                #adding columns for year, month
                district_table.insert(0,'year', year1)
                district_table.insert(1,'month', month1)
                district_table.insert(2,'state_name', row['state_name'])
                district_table.insert(3,'district_name', row['district_name'])

                #storing district table as csv

                district_table.to_csv(Path.joinpath(district_folder_path, f"{district_name_corrected}.csv"), index=False)

                print ("Scraped " + district + " table. Moving to next scraping task.")
     
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = { "download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
        print("csv for district "+district_name_corrected+ " exists. Moving to next scraping task.")

    #scraping block

    block_folder_path = Path.joinpath(district_folder_path, row["block_name"].lower().strip().replace(" ", "_"))
    block_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["block_name"].lower().strip().replace(" ", "_"))
    block_file_path = Path.joinpath(block_folder_path, f"{block_name_corrected}.csv")

    if not block_folder_path.exists():
        block_folder_path.mkdir(parents=True)

    if not block_file_path.exists():
        print("csv for block "+block_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:


                counter += 1

                driver.get(url)

                driver.implicitly_wait(5)

                #the first year and month

                selectyear1 = Select(driver.find_element(By.ID, 'yearId'))
                year1='2022' 
                selectyear1.select_by_value(year1)
                time.sleep(15)

                selectmonth1 = Select(driver.find_element(By.ID, 'month'))
                month1='07' 
                selectmonth1.select_by_value(month1)
                driver.implicitly_wait(5)

                #the second year and month

                selectyear2 = Select(driver.find_element(By.ID, 'yearIdd'))
                year2='2022' 
                selectyear2.select_by_value(year2)
                driver.implicitly_wait(5)

                selectmonth2 = Select(driver.find_element(By.ID, 'toMonth')) 
                month2='07'
                selectmonth2.select_by_value(month2)
                driver.implicitly_wait(5)

                #clicking submit

                driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
                driver.implicitly_wait(5)

                #selecting the state href web element
                state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):
                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)
                            
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + state + ". Scraping district names the next page." )

                        driver.implicitly_wait(5)

                        district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        district_names_next = [x.get_attribute("text") for x in district_page]
                        district_names.extend(district_names_next)
                        district_hrefs_next = [x.get_attribute("href") for x in district_page]
                        district_hrefs.extend(district_hrefs_next)

                    else:

                        break

                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}
                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(5)


                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 

                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                #retrieving the block table
                block_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                block_table_element_html = block_table_element.get_attribute("outerHTML")
                block_table= pd.read_html(block_table_element_html)
                block_table= block_table[0]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(5)

                        block_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        block_table_element_html_new = block_table_element_new.get_attribute("outerHTML")
                        block_table_new= pd.read_html(block_table_element_html_new)
                        block_table_new=block_table_new[0]
                        block_table.append(block_table_new)

                        driver.implicitly_wait(5)

                    else:

                        break                                                              


                #changing multiindex to single index

                block_table.columns = block_table.columns.droplevel(2)
                block_table.columns = block_table.columns.map('_'.join).str.strip('_')
                block_table.columns = [x.lower() for x in block_table.columns]
                block_table.columns = [x.replace(" ", "_").replace(".", "").replace("(rs)","_rs") for x in block_table.columns]
                block_table.rename(columns = {'sr_no_sr_no':'sr_no', 'grampanchayat_name_grampanchayat_name':'gp_name'}, inplace = True)
                block_table.drop('unnamed:_14_level_0_unnamed:_14_level_1', inplace=True, axis=1)
                block_table.drop('sr_no', inplace=True, axis=1)
                block_table.drop(block_table.index[-1], inplace=True, axis=0)

                #adding columns for year, month

                block_table.insert(0,'year', year1)
                block_table.insert(1,'month', month1)
                block_table.insert(2,'state_name', row['state_name'])
                block_table.insert(3,'district_name', row['district_name'])
                block_table.insert(4,'block_name', row['block_name'])
                

                #storing block table as csv

                block_table.to_csv(Path.joinpath(block_folder_path, f"{block_name_corrected}.csv"), index=False)

                print ("Scraped " + block + " table. Moving to next scraping task.")

            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
       print("csv for block "+block_name_corrected+ " exists. Moving to next scraping task.")

     #scraping gp

    gp_folder_path = Path.joinpath(block_folder_path, row["gp_name"].lower().strip().replace(" ", "_"))
    gp_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["gp_name"].lower().strip().replace(" ", "_"))
    gp_file_path = Path.joinpath(gp_folder_path, f"{gp_name_corrected}.csv")

    if not gp_folder_path.exists():
        gp_folder_path.mkdir(parents=True)

    if not gp_file_path.exists():
        print("csv for gp "+gp_name_corrected+" doesn't exist and proceeding for scraping.")

        counter = 0

        while counter <= 2:

            try:

                counter += 1

                driver.get(url)

                driver.implicitly_wait(5)

                #the first year and month

                selectyear1 = Select(driver.find_element(By.ID, 'yearId'))
                year1='2022' 
                selectyear1.select_by_value(year1)
                time.sleep(15)

                selectmonth1 = Select(driver.find_element(By.ID, 'month'))
                month1='07' 
                selectmonth1.select_by_value(month1)
                driver.implicitly_wait(5)

                #the second year and month

                selectyear2 = Select(driver.find_element(By.ID, 'yearIdd'))
                year2='2022' 
                selectyear2.select_by_value(year2)
                driver.implicitly_wait(5)

                selectmonth2 = Select(driver.find_element(By.ID, 'toMonth')) 
                month2='07'
                selectmonth2.select_by_value(month2)
                driver.implicitly_wait(5)

                #clicking submit

                driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
                driver.implicitly_wait(5)

                #selecting the state href web element
                state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
                state_names = [x.get_attribute("text") for x in state_page]
                state_hrefs = [x.get_attribute("href") for x in state_page]

                state_dict = {}

                for state, st_href in zip(state_names, state_hrefs):
                    if state == row["state_name"]:
                        state_dict = {state: st_href}
                driver.execute_script(state_dict[row["state_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)
                            
                district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                district_names = [x.get_attribute("text") for x in district_page]
                district_hrefs = [x.get_attribute("href") for x in district_page]

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + state + ". Scraping district names the next page." )

                        driver.implicitly_wait(5)

                        district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        district_names_next = [x.get_attribute("text") for x in district_page]
                        district_names.extend(district_names_next)
                        district_hrefs_next = [x.get_attribute("href") for x in district_page]
                        district_hrefs.extend(district_hrefs_next)

                    else:

                        break

                for district, di_href in zip(district_names, district_hrefs):
                    if district == row["district_name"]:
                        district_dict = {district: di_href}
                driver.execute_script(district_dict[row["district_name"]])
                driver.implicitly_wait(5)


                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                block_names = [x.get_attribute("text") for x in block_page]
                block_hrefs = [x.get_attribute("href") for x in block_page]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                        driver.implicitly_wait(5)

                        block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        block_names_next = [x.get_attribute("text") for x in block_page]
                        block_names.extend(block_names_next)
                        block_hrefs_next = [x.get_attribute("href") for x in block_page]
                        block_hrefs.extend(block_hrefs_next)                                        

                    else:

                        break 

                for block, bl_href in zip(block_names, block_hrefs):
                    if block == row["block_name"]:
                        block_dict = {block: bl_href}
                driver.execute_script(block_dict[row["block_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                gp_names = [x.get_attribute("text") for x in gp_page]
                gp_hrefs = [x.get_attribute("href") for x in gp_page]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                        driver.implicitly_wait(5)

                        gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                        gp_names_next = [x.get_attribute("text") for x in gp_page]
                        gp_names.extend(gp_names_next)
                        gp_hrefs_next = [x.get_attribute("href") for x in gp_page]
                        gp_hrefs.extend(gp_hrefs_next)
                        driver.implicitly_wait(5)

                    else:

                        break

                for gp, gp_href in zip(gp_names, gp_hrefs):
                    if gp == row["gp_name"]:
                        
                        gp_dict = {gp: gp_href}
                driver.execute_script(gp_dict[row["gp_name"]])
                driver.implicitly_wait(5)

                #selecting option to view 100 pages
                no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                no_of_pages.select_by_value("100")

                driver.implicitly_wait(5)

                #retrieving the gp table
                gp_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                gp_table_element_html = gp_table_element.get_attribute("outerHTML")
                gp_table= pd.read_html(gp_table_element_html)
                gp_table= gp_table[0]

                driver.implicitly_wait(5)

                while True:

                    NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                    next_page_class = NextButton.get_attribute("class")
                    if next_page_class != "paginate_button next disabled":
                        NextButton.click()
                        print("NEXT Button exists for " + gp + ". Scraping village names the next page." )

                        driver.implicitly_wait(5)

                        gp_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                        gp_table_element_html_new = gp_table_element_new.get_attribute("outerHTML")
                        gp_table_new= pd.read_html(gp_table_element_html_new)
                        gp_table_new=gp_table_new[0]
                        gp_table.append(gp_table_new)

                        driver.implicitly_wait(5)

                    else:

                        break                                                              


                #changing multiindex to single index

                gp_table.columns = gp_table.columns.droplevel(2)
                gp_table.columns = gp_table.columns.map('_'.join).str.strip('_')
                gp_table.columns = [x.lower() for x in gp_table.columns]
                gp_table.columns = [x.replace(" ", "_").replace(".", "").replace("(rs)","_rs") for x in gp_table.columns]
                gp_table.rename(columns = {'sr_no_sr_no':'sr_no', 'village_name_village_name':'village_name'}, inplace = True)
                gp_table.drop('unnamed:_14_level_0_unnamed:_14_level_1', inplace=True, axis=1)
                gp_table.drop('sr_no', inplace=True, axis=1)
                gp_table.drop(gp_table.index[-1], inplace=True, axis=0)

                #adding columns for year, month

                gp_table.insert(0,'year', year1)
                gp_table.insert(1,'month', month1)
                gp_table.insert(2,'state_name', row['state_name'])
                gp_table.insert(3,'district_name', row['district_name'])
                gp_table.insert(4,'block_name', row['block_name'])
                gp_table.insert(5,'gp_name',row['gp_name'])
                
                #storing gp table as csv

                gp_table.to_csv(Path.joinpath(gp_folder_path, f"{gp_name_corrected}.csv"), index=False)

                print ("Scraped " + gp + " table. Moving to next scraping task.")                      

                
            except (NoSuchElementException, TimeoutException, KeyError) as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']}")

                print("Calling driver again")

                if counter == 2:
                    break

            except WebDriverException as ex:

                print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']}")

                driver.close()

                print("Calling driver again")
                # defining Chrome options
                chrome_options = webdriver.ChromeOptions()
                prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                chrome_options.add_experimental_option("prefs", prefs)
                chrome_options.add_argument("start-maximized")

                driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    else:
       print("csv for gp "+gp_name_corrected+ " exists. Moving to next scraping task.")
    
    #scraping village

    if row["village_name"]!="NOT_FOUND":

        village_folder_path = Path.joinpath(gp_folder_path, row["village_name"].lower().strip().replace(" ", "_"))
        village_name_corrected = re.sub(r"[^A-Za-z0-9_]", "", row["village_name"].lower().strip().replace(" ", "_"))
        village_file_path = Path.joinpath(village_folder_path, f"{village_name_corrected}.csv")

        if not village_folder_path.exists():
            village_folder_path.mkdir(parents=True)

        if not village_file_path.exists():
            print("csv for village "+village_name_corrected+" doesn't exist and proceeding for scraping.")

            counter = 0

            while counter <= 2:

                try:
                    counter += 1

                    driver.get(url)

                    driver.implicitly_wait(5)

                    #the first year and month

                    selectyear1 = Select(driver.find_element(By.ID, 'yearId'))
                    year1='2022' 
                    selectyear1.select_by_value(year1)
                    time.sleep(15)

                    selectmonth1 = Select(driver.find_element(By.ID, 'month'))
                    month1='07' 
                    selectmonth1.select_by_value(month1)
                    driver.implicitly_wait(5)

                    #the second year and month

                    selectyear2 = Select(driver.find_element(By.ID, 'yearIdd'))
                    year2='2022' 
                    selectyear2.select_by_value(year2)
                    driver.implicitly_wait(5)

                    selectmonth2 = Select(driver.find_element(By.ID, 'toMonth')) 
                    month2='07'
                    selectmonth2.select_by_value(month2)
                    driver.implicitly_wait(5)

                    #clicking submit

                    driver.find_element(By.XPATH, '//*[@id="panelfilter"]/ul/li[4]/div/input[1]').click()
                    driver.implicitly_wait(5)

                    #selecting the state href web element
                    state_page = driver.find_elements(By.XPATH, '//*[@id="tablee"]//a')
                    state_names = [x.get_attribute("text") for x in state_page]
                    state_hrefs = [x.get_attribute("href") for x in state_page]

                    state_dict = {}

                    for state, st_href in zip(state_names, state_hrefs):
                        if state == row["state_name"]:
                            state_dict = {state: st_href}
                    driver.execute_script(state_dict[row["state_name"]])
                    driver.implicitly_wait(5)

                    #selecting option to view 100 pages
                    no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                    no_of_pages.select_by_value("100")

                    driver.implicitly_wait(5)
                                
                    district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                    district_names = [x.get_attribute("text") for x in district_page]
                    district_hrefs = [x.get_attribute("href") for x in district_page]

                    while True:

                        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                        next_page_class = NextButton.get_attribute("class")
                        if next_page_class != "paginate_button next disabled":
                            NextButton.click()
                            print("NEXT Button exists for " + state + ". Scraping district names the next page." )

                            driver.implicitly_wait(5)

                            district_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                            district_names_next = [x.get_attribute("text") for x in district_page]
                            district_names.extend(district_names_next)
                            district_hrefs_next = [x.get_attribute("href") for x in district_page]
                            district_hrefs.extend(district_hrefs_next)

                        else:

                            break

                    for district, di_href in zip(district_names, district_hrefs):
                        if district == row["district_name"]:
                            district_dict = {district: di_href}
                    driver.execute_script(district_dict[row["district_name"]])
                    driver.implicitly_wait(5)


                    #selecting option to view 100 pages
                    no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                    no_of_pages.select_by_value("100")

                    driver.implicitly_wait(5)

                    block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                    block_names = [x.get_attribute("text") for x in block_page]
                    block_hrefs = [x.get_attribute("href") for x in block_page]

                    driver.implicitly_wait(5)

                    while True:

                        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                        next_page_class = NextButton.get_attribute("class")
                        if next_page_class != "paginate_button next disabled":
                            NextButton.click()
                            print("NEXT Button exists for " + district + ". Scraping block names the next page." )

                            driver.implicitly_wait(5)

                            block_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                            block_names_next = [x.get_attribute("text") for x in block_page]
                            block_names.extend(block_names_next)
                            block_hrefs_next = [x.get_attribute("href") for x in block_page]
                            block_hrefs.extend(block_hrefs_next)                                        

                        else:

                            break 

                    for block, bl_href in zip(block_names, block_hrefs):
                        if block == row["block_name"]:
                            
                            block_dict = {block: bl_href}
                    driver.execute_script(block_dict[row["block_name"]])
                    driver.implicitly_wait(5)

                    #selecting option to view 100 pages
                    no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                    no_of_pages.select_by_value("100")

                    driver.implicitly_wait(5)

                    gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                    gp_names = [x.get_attribute("text") for x in gp_page]
                    gp_hrefs = [x.get_attribute("href") for x in gp_page]

                    driver.implicitly_wait(5)

                    while True:

                        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                        next_page_class = NextButton.get_attribute("class")
                        if next_page_class != "paginate_button next disabled":
                            NextButton.click()
                            print("NEXT Button exists for " + block + ". Scraping gp names the next page." )

                            driver.implicitly_wait(5)

                            gp_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                            gp_names_next = [x.get_attribute("text") for x in gp_page]
                            gp_names.extend(gp_names_next)
                            gp_hrefs_next = [x.get_attribute("href") for x in gp_page]
                            gp_hrefs.extend(gp_hrefs_next)
                            driver.implicitly_wait(5)

                        else:

                            break

                    for gp, gp_href in zip(gp_names, gp_hrefs):
                        if gp == row["gp_name"]:
                            
                            gp_dict = {gp: gp_href}
                    driver.execute_script(gp_dict[row["gp_name"]])
                    driver.implicitly_wait(5)

                    #selecting option to view 100 pages
                    no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                    no_of_pages.select_by_value("100")

                    driver.implicitly_wait(5)

                    village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                    village_names = [x.get_attribute("text") for x in village_page]
                    village_hrefs = [x.get_attribute("href") for x in village_page]

                    driver.implicitly_wait(5)

                    while True:

                        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                        next_page_class = NextButton.get_attribute("class")
                        if next_page_class != "paginate_button next disabled":
                            NextButton.click()
                            print("NEXT Button exists for " + gp + ". Scraping village names the next page." )

                            driver.implicitly_wait(5)

                            village_page = driver.find_elements(By.XPATH, "//*[@id='example']//a")
                            village_names_next = [x.get_attribute("text") for x in village_page]
                            village_names.extend(gp_names_next)
                            village_hrefs_next = [x.get_attribute("href") for x in village_page]
                            village_hrefs.extend(gp_hrefs_next)

                            driver.implicitly_wait(5)

                        else:

                            break 
                    
                    for village, vi_href in zip(village_names, village_hrefs):
                        if village == row["village_name"]:
                            
                            village_dict = {village: vi_href}
                    driver.execute_script(village_dict[row["village_name"]])
                    driver.implicitly_wait(5)

                    #selecting option to view 100 pages
                    no_of_pages = Select(driver.find_element(By.XPATH, '//*[@id="example_length"]/label/select'))
                    no_of_pages.select_by_value("100")

                    driver.implicitly_wait(5)

                    #retrieving the village table
                    village_table_element = driver.find_element(By.XPATH, '//table[@id="example"]')
                    village_table_element_html = village_table_element.get_attribute("outerHTML")
                    village_table= pd.read_html(village_table_element_html)
                    village_table= village_table[0]

                    driver.implicitly_wait(5)

                    while True:

                        NextButton = driver.find_element(By.XPATH, '//*[@id="example_next"]')
                        next_page_class = NextButton.get_attribute("class")
                        if next_page_class != "paginate_button next disabled":
                            NextButton.click()
                            print("NEXT Button exists for " + village + ". Scraping shg names the next page." )

                            driver.implicitly_wait(5)

                            village_table_element_new = driver.find_element(By.XPATH, '//table[@id="example"]')
                            village_table_element_html_new = village_table_element_new.get_attribute("outerHTML")
                            village_table_new= pd.read_html(village_table_element_html_new)
                            village_table_new=village_table_new[0]
                            village_table.append(village_table_new)

                            driver.implicitly_wait(5)

                        else:

                            break                                                              


                    #changing multiindex to single index

                    village_table.columns = village_table.columns.droplevel(1)
                    village_table.columns = ["sr_no", "self_help_group", "disbursed_by", "fund_type", "source_of_fund", "bank", "branch", "account_no","amount_in_rs","mode_of_payment", "payment_ref_no", "release_date", "remarks"]
                    village_table.drop('sr_no', inplace=True, axis=1)
                    village_table.drop(village_table.index[-1], inplace=True, axis=0)                                                

                    #adding columns for year, month

                    village_table.insert(0,'year', year1)
                    village_table.insert(1,'month', month1)
                    village_table.insert(2,'state_name', row['state_name'])
                    village_table.insert(3,'district_name', row['district_name'])
                    village_table.insert(4,'block_name', row['block_name'])
                    village_table.insert(5,'gp_name',row['gp_name'])
                    village_table.insert(6,'village_name',row['village_name'])
                    
                    #storing gp table as csv

                    village_table.to_csv(Path.joinpath(village_folder_path, f"{village_name_corrected}.csv"), index=False)

                    print ("Scraped " + village + " table. Moving to next scraping task.")


                except (NoSuchElementException, TimeoutException, KeyError) as ex:

                    print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['village_name']}")

                    print("Calling driver again")

                    if counter == 2:
                        break

                except WebDriverException as ex:

                    print(f"{ex}: Raised at {row['state_name']} {row['district_name']} {row['block_name']} {row['gp_name']} {row['village_name']}")

                    driver.close()

                    print("Calling driver again")
                    # defining Chrome options
                    chrome_options = webdriver.ChromeOptions()
                    prefs = {"download.default_directory": str(dir_path),"profile.default_content_setting_values.automatic_downloads": 1,}
                    chrome_options.add_experimental_option("prefs", prefs)
                    chrome_options.add_argument("start-maximized")

                    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

        else:
            print("csv for village "+village_name_corrected+ " exists. Moving to next scraping task.")
    else:
        print("There is no village hyperlink.")

driver.close()

print("Looping has ended. Scraper rests.")
  
