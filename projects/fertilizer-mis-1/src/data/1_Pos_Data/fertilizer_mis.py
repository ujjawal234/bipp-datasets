import calendar
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import scrapy
from dateutil.relativedelta import relativedelta
from dateutil.rrule import DAILY, rrule
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class Fertilizermisscrapper(scrapy.Spider):
    name = "fertilizermis"
    page_number = 1
    final_table = pd.DataFrame()
    monthly_table = pd.DataFrame()
    project_dir = str(Path(__file__).resolve().parents[0])
    parent_folder = project_dir + "/data/raw/"
    dataset = []

    def start_requests(self):
        # this is the request that will initiate the scraping
        yield Request("https://reports.dbtfert.nic.in/mfmsReports/getPOSReportForm")

    def parse(self, response):
        "This fuction will parse the names of all states in the district and will raise another request to get all districts in a state."

        state_names = response.xpath(
            '//select[@id="parameterStateName"]/option/@value'
        ).extract()
        # print(state_names)
        for state in state_names[1:]:
            yield FormRequest(
                url="https://reports.dbtfert.nic.in/mfmsReports/getDistrictList",
                method="POST",
                formdata={"selectedStateName": state},
                meta={"State": state},
                callback=self.dist_parser,
            )

    def dist_parser(self, response):
        "This fuction will parse the names of the districts and will raise another request for illing the dates."

        districts = json.loads(response.text)
        district_names = districts.values()
        dist_names = list(district_names)
        for dist in dist_names[1:]:
            self.final_table = self.final_table.iloc[0:0]
            current = datetime(2018, 1, 1)
            start_date = datetime(2017, 1, 1)
            for i in list(rrule(DAILY, dtstart=start_date, until=current)):
                j = i + relativedelta(day=1)
                yield Request(
                    "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action?parameterFromDate={}%2F{}%2F{}&parameterDistrictName={}&d-6849390-p=1&parameterStateName={}&parameterToDate={}%2F{}%2F{}".format(
                        i.strftime("%d"),
                        i.strftime("%m"),
                        i.strftime("%Y"),
                        dist,
                        response.meta["State"],
                        j.strftime("%d"),
                        j.strftime("%m"),
                        j.strftime("%Y"),
                    ),
                    method="Post",
                    meta={
                        "State": response.meta["State"],
                        "District": dist,
                        "From Date": i.strftime("%d/%m/%Y"),
                        "To Date": j.strftime("%d/%m/%Y"),
                        "From Year": i.strftime("%Y"),
                        "From month": i.strftime("%m"),
                        "From day": i.strftime("%d"),
                        "To Year": j.strftime("%Y"),
                        "To month": j.strftime("%m"),
                        "To Day": j.strftime("%d"),
                    },
                    callback=self.get_data,
                )

    def get_data(self, response):
        "This fuction saves the csv file in the defined path."

        try:
            table = response.css("#districtTable").get()
            table_list = pd.read_html(table)
            data = table_list[0]
            self.final_table = pd.concat([self.final_table, data], sort=False)
            print(self.final_table)
            self.pagination(response)

        except TypeError:
            self.dataset.append(response.meta)

    def pagination(self, response):
        isexists = response.css("span.pagelinks strong+a::(href)").extract(
            default="not-found"
        )
        if isexists == "not-found":
            self.monthly(response)
        else:
            url = response.css("span.pagelinks strong+a::(href)").get()
            url_match = url.split("&")
            url_match1 = url_match[2]
            absolute_url = (
                "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action"
            )
            if url_match1 != "d-6849390-p=1":
                final_url = urljoin(absolute_url, url)
                yield Request(
                    final_url,
                    meta={
                        "State": response.meta["State"],
                        "District": response.meta["District"],
                        "From Date": response.meta["From Date"],
                        "To Date": response.meta["To Date"],
                        "From Year": response.meta["From Year"],
                        "From month": response.meta["From month"],
                        "From day": response.meta["From day"],
                        "To Year": response.meta["To Year"],
                        "To month": response.meta["To month"],
                        "To Day": response.meta["To Day"],
                    },
                    callback=self.get_data,
                )
            else:
                self.monthly(response)

    def monthly(self, response):
        if response.meta["To Day"] == "1":
            self.monthly_table = pd.concat(
                [self.monthly_table, self.final_table], sort=False
            )
            meta_data = dict(response.meta)
            month_number = meta_data.get("From month")
            month = calendar.month_name[int(month_number)]
            file_path = (
                self.parent_folder
                + "/"
                + meta_data.get("State")
                + "/"
                + meta_data.get("District")
            )
            file_name = meta_data.get("From Year") + "_" + month + ".csv"
            self.directory(file_path)
            self.monthly_table.to_csv(file_path + "/" + file_name)
            self.monthly_table = self.monthly_table.iloc[0:0]
        else:
            self.monthly_table = pd.concat(
                [self.monthly_table, self.final_table], sort=False
            )

    def directory(self, file_path):
        path_parts = file_path.split("/")
        for i in range(1, len(path_parts) + 1):
            present_path = "/".join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)


def main():

    process = CrawlerProcess()
    process.crawl(Fertilizermisscrapper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
