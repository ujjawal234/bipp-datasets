from pathlib import Path

import pandas as pd
import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class psdataOrrisascraper(scrapy.Spider):
    name = "psdataOrrisa"
    custom_settings = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    }

    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"

    def start_requests(self):
        # request to initiate the scraping
        yield Request("http://election.ori.nic.in/odishaceo/ViewEroll.aspx")

    def parse(self, response):
        # "This fuction will parse the names of all the districts in the state and will raise another request to get all ACS in the district."

        # dist_names = response.xpath(
        # '//select[@id="ddlDistrict"]/option/text()'
        # ).extract()
        # print(dist_names)

        dist_values = response.xpath(
            '//select[@id="ddlDistrict"]/option/@value'
        ).extract()
        form_dict = {
            "__EVENTTARGET": response.css(
                "#__EVENTTARGET::attr(value)"
            ).extract_first(),
            "__EVENTARGUMENT": response.css(
                "#__EVENTARGUMENT::attr(value)"
            ).extract_first(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract_first(),
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract_first(),
            "__VIEWSTATEGENERATOR": response.css(
                "#__VIEWSTATEGENERATOR::attr(value)"
            ).extract_first(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract_first(),
            "ddlDistrict": "",
            "TextCaptcha": "",
        }
        print(form_dict)
        i = 1
        # //*[@id="ddlAC"]/option
        for dist in dist_values[1:]:
            form_dict["ddlDistrict"] = dist
            yield FormRequest.from_response(
                response,
                url="http://election.ori.nic.in/odishaceo/ViewEroll.aspx",
                method="POST",
                formdata=form_dict,
                # dont_click="True",
                callback=self.ac_parser,
                meta={"district_code": dist},
            )
            i += 1

    def ac_parser(self, response):

        ac_list = response.xpath('//select[@id="ddlAC"]/option/text()').extract()
        ac_values = response.xpath('//select[@id="ddlAC"]/option/@value').extract()
        # print(ac_list)
        # print("*****")
        form_dict = {
            "__EVENTTARGET": response.css(
                "#__EVENTTARGET::attr(value)"
            ).extract_first(),
            "__EVENTARGUMENT": response.css(
                "#__EVENTARGUMENT::attr(value)"
            ).extract_first(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract_first(),
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract_first(),
            "__VIEWSTATEGENERATOR": response.css(
                "#__VIEWSTATEGENERATOR::attr(value)"
            ).extract_first(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract_first(),
            "ddlDistrict": response.meta["district_code"],
            "ddlAC": "",
            "TextCaptcha": "",
        }
        i = 1
        for ac in ac_values[1:]:
            form_dict["ddlAC"] = ac
            yield FormRequest.from_response(
                response,
                url="http://election.ori.nic.in/odishaceo/ViewEroll.aspx",
                method="POST",
                formdata=form_dict,
                meta={"ac_names": ac_list[i]},
                # dont_click="True",
                callback=self.save_data,
            )
            i += 1

    def save_data(self, response):
        final_table = response.xpath('//select[@id="ddlPart"]/option/text()').extract()
        # print(final_table)
        table_list = pd.DataFrame(final_table, columns=["Polling_Station_Name"])
        table_list = table_list.iloc[1:, 0:]

        file_path = (
            self.parent_folder + "/" + "3_Orrisa" + "/" + response.meta["ac_names"]
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
    process.crawl(psdataOrrisascraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
