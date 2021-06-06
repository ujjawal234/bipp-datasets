from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.expected_conditions import staleness_of
from selenium.webdriver.chrome.options import Options
driver = webdriver.Chrome(executable_path=r"C:\Users\91987\Downloads\chromedriver_win32\chromedriver.exe")
driver.get("https://reports.dbtfert.nic.in/mfmsReports/getDealerInfoExcel")
drp_state=Select(driver.find_element_by_id("parameterStateName"))    #dropdown element which has the list of states 
no_of_states=len(drp_state.options)    #counts the no. of states
for x in range(1,no_of_states):
    ignored_exceptions=(NoSuchElementException,StaleElementReferenceException)
    your_element = WebDriverWait(driver,5,ignored_exceptions=ignored_exceptions)\
                        .until(expected_conditions.presence_of_element_located((By.ID,"parameterStateName")))
    your_element = WebDriverWait(driver,5,ignored_exceptions=ignored_exceptions)\
                        .until(expected_conditions.presence_of_element_located((By.ID,"parameterDistrictName")))
    drp_state=Select(driver.find_element_by_id("parameterStateName"))    #dropdown list which has names of states
    drp_district=Select(driver.find_element_by_id("parameterDistrictName"))   #dropdown list of districts
    drp_state.select_by_index(x)    #selects each state by index
    drp_district.select_by_index(0)    #selects first option from district dropdown so that all districts data gets stored in one csv file
    driver.find_element_by_id("exporttoexcel").click()
    if x==no_of_states-1:
        exit()