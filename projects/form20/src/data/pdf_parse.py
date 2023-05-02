from pathlib import Path
from time import sleep

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager


class pdf_parser:

    """
    Class for running SMALLPDF web based OCR on Form 20 files.
    """

    def __init__(self):

        """Constructor function to initiate the driver and define necessary directories"""

        # defining directories
        self.dir_path = Path.cwd()
        self.raw_path = Path.joinpath(self.dir_path, "data", "raw")
        self.interim_path = Path.joinpath(self.dir_path, "data", "interim")
        self.external_path = Path.joinpath(self.dir_path, "data", "external")

        # defining Chrome options
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": str(self.dir_path),
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("start-maximized")

        self.driver = webdriver.Chrome(
            ChromeDriverManager().install(), options=chrome_options
        )

        url = "https://smallpdf.com/pdf-converter"

        self.driver.get(url)

        sleep(2)

    def file_uploader(self):

        return None


def main():
    # pdf = pdf_parser()
    return None


if __name__ == "__main__":
    main()
