Please use the script named, "fl2_name_scraper_test.py" for fixing the explicit waits issues.

The scraper doesn't need any external file, once you pull the "nrlm-farm-live" project from git.

All the names extracted from dropdowns will be stored as json files in the respective parent folders. The script shall ensure
presence of these folders and may create these paths if not existing.

Before running the script, please note that since the explicit waits weren't working, I have supplied year value manually 
in line number 51 of this script. If you can make the explicit waits work, you can change this process and loop the entire process
over the different year values as well.

