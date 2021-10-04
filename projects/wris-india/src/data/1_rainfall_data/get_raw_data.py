# %%
# importing the libraries
# import bs4
# import pandas as pd
# import selenium
from pathlib import Path
from sys import exit, platform

# %%
# Select the chrome driver based on the OS
# The chrome driver will also depend on the version of the chrome running on the os
driver_dir = str(Path(__file__).resolve().parents[0]) + "/chromedrivers/"
if platform == "linux" or platform == "linux2":
    driver_path = driver_dir + "linux/chromedriver"
elif platform == "win32":
    driver_path = driver_dir + "windows/chromedriver.exe"
elif platform == "darwin":
    driver_path = driver_dir + "mac/chromedriver"
else:
    exit("OS not supported. Exiting the program")
