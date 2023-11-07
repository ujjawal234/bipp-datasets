#importing the libraries
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup 
import time 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lxml import etree
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
import time
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime, timedelta
import requests
import numpy as np
from selenium.webdriver.support.ui import WebDriverWait
import csv

subdirectory_august = 'august_retail_prices/'
subdirectory_september = 'september_retail_prices/'
# Create the subdirectory if it doesn't exist
if not os.path.exists(subdirectory_august):
    os.makedirs(subdirectory_august)
if not os.path.exists(subdirectory_september):
    os.makedirs(subdirectory_september)


# Loop through days in August
for x in range(5):
    for y in range(7):
        # Skip certain conditions
        if ((x == 0) and (y <=1)) or ((x == 4) and (y >=5)):
            continue
        # Define the URL to scrape
        try:
                link = 'https://fcainfoweb.nic.in/reports/report_menu_web.aspx'
                driver = webdriver.Chrome()
                driver.get(link)
                driver.maximize_window()
                #doin the necessity on the web page


                # clicking on price report
                we = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Rbl_Rpt_type_0"]')
                we.click()

                # selecting "daily prices" from dropdown
                we_drp = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Ddl_Rpt_Option0"]')
                drp = Select(we_drp)
                drp.select_by_index(1)

                #selcting the date as 1st october
                we_date = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Txt_FrmDate"]') #selcting the date dropdown
                we_date.click()

                #selcting the month as august
                select_august =  driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_CalendarExtender_frmdate_prevArrow"]')
                time.sleep(1)
                select_august.click()
                time.sleep(1)
                select_august.click()
                time.sleep(1)
                #selcting the dates in august
                we_d1 = driver.find_element(By.XPATH,f'//*[@id="ctl00_MainContent_CalendarExtender_frmdate_day_{x}_{y}"]')

                we_d1.click() 

                #clicking on get data
                we_get_data = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_btn_getdata1"]')
                we_get_data.click()
                wait = WebDriverWait(driver, 20)  # Wait for up to 20 seconds
                #scrapping the data into a table 
                page_html = driver.page_source

                table_data = []
                row_data=[]

                soup = BeautifulSoup(page_html, 'lxml')
                table = soup.find('table',style="margin:auto;")
                dates= table.find('thead').find('tbody').find('b').text
                table_data.append(dates)
                colhead= table.find('table', id='gv0').find('thead').find('tr')
                for i in colhead.find_all('th'):
                    row_data.append(i.text)
                table_data.append(row_data)
                row_data=[]
                colbody= table.find('table', id='gv0').find('tbody')

                for a in colbody.find_all('tr'):
                    for i in a.find_all('td'):
                        row_data.append(i.text)
                    table_data.append(row_data)
                    row_data=[]
                tablepd=pd.DataFrame(table_data[6:],columns=table_data[1])
                #Handling missing values
                for column in tablepd.columns:
                    if (column=='States/UTs'):
                        continue
                    tablepd[column] = pd.to_numeric(tablepd[column], errors='coerce')
                    tablepd[column].fillna(tablepd[column].mean(), inplace=True)
                #saving the csv files
                name= subdirectory_august+ 'retail_prices_2023-08-'+table_data[0][6:8]+'.csv'
                tablepd.to_csv(name,index=False)
                time.sleep(1)
                # closing the driver
                driver.quit()
        except Exception as e:
                # Handle any exceptions here
                print(f"An error occurred: {str(e)}")
                


# Loop through days in September
for x in range(5):
    for y in range(7):
        # Skip certain conditions
        if ((x == 0) and (y <=4))  :
            continue
        try:
                link = 'https://fcainfoweb.nic.in/reports/report_menu_web.aspx'
                driver = webdriver.Chrome()
                driver.get(link)
                driver.maximize_window()
                #doin the necessity on the web page


                # clicking on price report
                we = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Rbl_Rpt_type_0"]')
                we.click()

                # selecting "daily prices" from dropdown
                we_drp = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Ddl_Rpt_Option0"]')
                drp = Select(we_drp)
                drp.select_by_index(1)

                #selcting the date as 1st october
                we_date = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_Txt_FrmDate"]') #selcting the date dropdown
                we_date.click()

                #selcting the month as august
                select_september =  driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_CalendarExtender_frmdate_prevArrow"]')
                time.sleep(1)
                select_september.click()
                time.sleep(1)
                #selcting the dates in august
                we_d1 = driver.find_element(By.XPATH,f'//*[@id="ctl00_MainContent_CalendarExtender_frmdate_day_{x}_{y}"]')

                we_d1.click() 

                #clicking on get data
                we_get_data = driver.find_element(By.XPATH, '//*[@id="ctl00_MainContent_btn_getdata1"]')
                we_get_data.click()
                wait = WebDriverWait(driver, 20)  # Wait for up to 20 seconds
                #scrapping the data into a table 
                page_html = driver.page_source

                table_data = []
                row_data=[]

                soup = BeautifulSoup(page_html, 'lxml')
                table = soup.find('table',style="margin:auto;")
                dates= table.find('thead').find('tbody').find('b').text
                table_data.append(dates)
                colhead= table.find('table', id='gv0').find('thead').find('tr')
                for i in colhead.find_all('th'):
                    row_data.append(i.text)
                table_data.append(row_data)
                row_data=[]
                colbody= table.find('table', id='gv0').find('tbody')

                for a in colbody.find_all('tr'):
                    for i in a.find_all('td'):
                        row_data.append(i.text)
                    table_data.append(row_data)
                    row_data=[]
                tablepd=pd.DataFrame(table_data[6:],columns=table_data[1])
                #Handling missing values
                for column in tablepd.columns:
                    if (column=='States/UTs'):
                        continue
                    tablepd[column] = pd.to_numeric(tablepd[column], errors='coerce')
                    tablepd[column].fillna(tablepd[column].mean(), inplace=True)
                #saving the csv files
                name= subdirectory_september+ 'retail_prices_2023-09-'+table_data[0][6:8]+'.csv'
                tablepd.to_csv(name,index=False)
                time.sleep(1)
                # closing the driver
                driver.quit()
        except Exception as e:
                # Handle any exceptions here
                print(f"An error occurred: {str(e)}")
                