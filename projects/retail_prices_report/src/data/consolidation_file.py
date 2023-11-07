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
# Define the directory where your CSV files are located
# Get a list of CSV files in the directory
csv_files = [filename for filename in os.listdir(subdirectory_august) if filename.endswith('.csv')]

# Define the output CSV file where you want to consolidate the content
output_file = 'retail_prices_aug_sep_2023.csv'

# Initialize an empty list to store the consolidated data
consolidated_data = []

# Iterate through the CSV files of august
for filename in csv_files:
    # Extract the date from the file title
    date = filename.replace('retail_prices_', '').replace('.csv', '')

    
    with open(os.path.join(subdirectory_august, filename), 'r', newline='') as csv_file:
        header = next(csv.reader(csv_file))  # Read the header from the CSV file
        rows = list(csv.reader(csv_file))  # Read the data rows from the CSV file

        # Add the date row at the beginning
        date_row = [date] + [''] * (len(header) - 1)
        consolidated_data.append(date_row)
        consolidated_data.append(header)
        consolidated_data.extend(rows)
csv_files = [filename for filename in os.listdir(subdirectory_september) if filename.endswith('.csv')]

# Iterate through the CSV files of september
for filename in csv_files:
    # Extract the date from the file title
    date = filename.replace('retail_prices_', '').replace('.csv', '')

    
    with open(os.path.join(subdirectory_september, filename), 'r', newline='') as csv_file:
        header = next(csv.reader(csv_file))  # Read the header from the CSV file
        rows = list(csv.reader(csv_file))  # Read the data rows from the CSV file

        # Add the date row at the beginning
        date_row = [date] + [''] * (len(header) - 1)
        consolidated_data.append(date_row)
        consolidated_data.append(header)
        consolidated_data.extend(rows)
        
# Write the consolidated data to the output CSV file
with open(output_file, 'w', newline='') as output_csv:
    csv_writer = csv.writer(output_csv)
    csv_writer.writerows(consolidated_data)

print("CSV consolidation complete.")
