from pathlib import Path

import pandas as pd
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess


class psdataWBscraper(scrapy.Spider):
    name = "psdataWB"
    project_dir = str(Path(__file__).resolve().parents[2])
    parent_folder = project_dir + "/data/raw/"
    print(parent_folder)
    start_urls = ["https://www.elections.in/west-bengal/polling-booths/"]

    def parse(self, response):
        # This function extracts links to various assembly constituencies in West_Bengal and gets the ps_data table for each constituency
        table_ps = response.css(
            "td:nth-child(8) a::attr(href) , td:nth-child(6) a::attr(href) , td:nth-child(4) a::attr(href) , td:nth-child(2) a::attr(href)"
        ).extract()

        ac_names = response.css(
            "td:nth-child(8) a::text , td:nth-child(6) a::text , td:nth-child(4) a::text , td:nth-child(2) a::text"
        ).extract()

        i = 0
        for ac in table_ps:

            yield Request(
                ac, meta={"ac_names": ac_names[i]}, callback=self.save_data
            )
            i += 1

    def save_data(self, response):
        # This function collects the final response, checks for any exceptions and saves the data of each constituency in csv format. The exception case data is stored in a .txt file.
        final_table = response.css("table.tableizer-table").get()

        if final_table:
            try:
                table_list = pd.read_html(final_table)
                df = table_list[0]
                data = df.iloc[:, 0:2]

                file_path = (
                    self.parent_folder
                    + "/"
                    + "1_West Bengal"
                    + "/"
                    + response.meta["ac_names"]
                )
                file_name = response.meta["ac_names"] + ".csv"
                self.directory(file_path)
                data.to_csv(file_path + "/" + file_name)

            except TypeError:
                pass

        else:
            text_data = response.css(".left-text").extract()
            text_list = pd.DataFrame(text_data)
            # print(text_data)

            file_path = (
                self.parent_folder
                + "/"
                + "1_WestBengal"
                + "/"
                + response.meta["ac_names"]
            )
            file_name = response.meta["ac_names"] + ".txt"
            self.directory(file_path)
            text_list.to_string(file_path + "/" + file_name)

    def directory(self, file_path):
        # This function creates directory and appropriate file path to save the data.
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


def main():

    process = CrawlerProcess()
    process.crawl(psdataWBscraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
