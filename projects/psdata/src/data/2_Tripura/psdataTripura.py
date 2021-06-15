from pathlib import Path

import pandas as pd
import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class psdataTripurascraper(scrapy.Spider):
    name = "psdataTripura"

    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"

    def start_requests(self):
        # request to initiate the scraping
        yield Request("http://ermstripura.nic.in/ERollTrp/ERollSearch2019Final.aspx")

    def parse(self, response):
        # "This fuction will parse the names of all the assembly constituency in the district and will raise another request to get all polling stations in the constituency."

        ac_names = response.xpath('//select[@id="ddACNoName"]/option/text()').extract()
        ac_values = response.xpath('//select[@id="ddACNoName"]/option/@value').extract()
        # print(ac_values)
        i = 1
        for ac in ac_values[1:]:
            yield FormRequest.from_response(
                response,
                url="http://ermstripura.nic.in/ERollTrp/ERollSearch2019Final.aspx",
                method="POST",
                formdata={"ddACNoName": ac},
                dont_click="True",
                meta={"ac_names": ac_names[i]},
                callback=self.save_data,
            )
            i += 1

    def save_data(self, response):
        final_table = response.xpath(
            '//select[@id="ddPartNoName"]/option/text()'
        ).extract()
        table_list = pd.DataFrame(final_table, columns=["Polling_Station_Name"])
        table_list = table_list.iloc[1:, 0:]

        file_path = (
            self.parent_folder + "/" + "2_Tripura" + "/" + response.meta["ac_names"]
        )
        file_name = response.meta["ac_names"] + ".csv"
        self.directory(file_path)
        table_list.to_csv(file_path + "/" + file_name, index=False)

    def directory(self, file_path):
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


def main():

    process = CrawlerProcess()
    process.crawl(psdataTripurascraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
