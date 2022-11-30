from pathlib import Path

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# defining directories
dir_path = Path.cwd()

# defining Chrome options
chrome_options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": str(dir_path),
    "profile.default_content_setting_values.automatic_downloads": 1,
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("--ignore-certificate-errors")

driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

# calling in the driver

url = "https://eci.gov.in/statistical-report/link-to-form-20/"
driver.get(url)

table_elem = driver.find_element(
    By.XPATH, '//table[@class="table table-striped table-hover table-bordered_"]'
)
table_html = table_elem.get_attribute("outerHTML")
df = pd.read_html(table_html)[0]
df = df.iloc[1:, :]
df.columns = ["states", "years"]


# creating election column
df["election"] = np.where(df["years"].isna(), np.nan, df["states"])
df.drop("years", inplace=True, axis=1)
df["states"] = np.where(df["election"].isna(), df["states"], np.nan)
df["states"].fillna(method="pad", axis=0, inplace=True)
df.dropna(axis=0, inplace=True)


print(df)
href_links = driver.find_elements(
    By.XPATH, '//table[@class="table table-striped table-hover table-bordered_"]//a'
)
href_list = [x.get_attribute("href") for x in href_links]
year_list = [x.get_attribute("text") for x in href_links]
# print(href_list)
