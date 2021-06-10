from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.expected_conditions import staleness_of
from selenium.webdriver.chrome.options import Options
import os
import shutil
options = webdriver.ChromeOptions()
options.add_experimental_option("prefs", {"download.default_directory": r"C:\Users\91987\Downloads\python\dealer_data"})
driver = webdriver.Chrome(executable_path=r"C:\Users\91987\Downloads\chromedriver_win32\chromedriver.exe",chrome_options=options)
driver.get("https://reports.dbtfert.nic.in/mfmsReports/getDealerInfoExcel")
drp_state=Select(driver.find_element_by_id("parameterStateName"))    #dropdown element which has the list of states 
no_of_states=len(drp_state.options)    #counts the no. of states

#i=-1
#print(i)
#print(states_list)
# for state in states_list:
#     drp_state=Select(driver.find_element_by_id("parameterStateName"))    #dropdown element which has the list of states 
#     print(state.text)
    
for x in range(1,no_of_states):
    ignored_exceptions=(NoSuchElementException,StaleElementReferenceException)
    your_element = WebDriverWait(driver,5,ignored_exceptions=ignored_exceptions)\
                        .until(expected_conditions.presence_of_element_located((By.ID,"parameterStateName")))
    your_element = WebDriverWait(driver,5,ignored_exceptions=ignored_exceptions)\
                        .until(expected_conditions.presence_of_element_located((By.ID,"parameterDistrictName")))
    drp_state=Select(driver.find_element_by_id("parameterStateName"))    #dropdown list which has names of states
    drp_state.select_by_index(x)    #selects each state by index
    driver.find_element_by_id("exporttoexcel").click()
i=-2
#print(i)
drp_state=Select(driver.find_element_by_id("parameterStateName"))
states_list = drp_state.options
for state in states_list:
    i=i+1
    drp_state=Select(driver.find_element_by_id("parameterStateName"))
    state_name = state.text
    new_file_name = state_name + '.csv'
    if(i==0):
        old_file_name = "Report.csv"
    elif(i>=1):
        old_file_name = "Report ({}).csv".format(i)
    if(i>=0):
        old_file = os.path.join(r"C:\Users\91987\Downloads\python\dealer_data", old_file_name)
        new_file = os.path.join(r"C:\Users\91987\Downloads\python\dealer_data", new_file_name)
        os.rename(old_file, new_file)

print(i)