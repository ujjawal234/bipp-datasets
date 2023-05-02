import requests
from bs4 import BeautifulSoup as bs
from lxml import etree


class form20:
    """
    Class for scraping Form 20 PDF at polling booth level
    """

    def __init__(self):
        """Initializes the scraper"""

        url = "https://eci.gov.in/statistical-report/link-to-form-20/"
        self.response = requests.get(url=url).text

    def href_grab(self):
        """
        HTML Parser to fetch hrefs for each state.

        """

        soup = bs(self.response, "html.parser")
        xpath_firendly_soup = etree.HTML(str(soup))
        # /html/body/main/div[3]/div/div[1]/div/div/table/tbody/tr[3]/td[2]/a[1]

        # looping over n number of rows looking for state and related election hrefs

        for i in range(0, 1, 105):

            rows = xpath_firendly_soup.xpath(f"//tr[{i}]//td[1]")

            for x in rows:
                print(x)


def main():
    scraper = form20()
    scraper.href_grab()


if __name__ == "__main__":
    main()
