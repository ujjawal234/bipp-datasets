import json
import scrapy
import pandas as pd
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess
from datetime import datetime
from dateutil.rrule import rrule,MONTHLY
from dateutil.relativedelta import relativedelta
from pathlib import Path
import calendar
class Fertilizermisscrapper(scrapy.Spider):
    name = "fertilizermis"
    page_number = 1
    final_table = pd.DataFrame()
    project_dir = str(Path(__file__).resolve().parents[0])
    parent_folder = project_dir + "/data/raw/"
    dataset = []
    def start_requests(self):
        # this is the request that will initiate the scraping
        yield Request(
            "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportForm"
        )

    def parse(self, response):
        "This fuction will parse the names of all states in the district and will raise another request to get all districts in a state."

        state_names = response.xpath('//select[@id="parameterStateName"]/option/@value').extract()
       # print(state_names)
        for state in state_names[1:]:
            yield FormRequest(
                url= "https://reports.dbtfert.nic.in/mfmsReports/getDistrictList",
                method="POST", formdata={"selectedStateName": state}, meta={"State": state},
                callback=self.dist_parser,
            )

    def dist_parser(self, response):
        "This fuction will parse the names of the districts and will raise another request for illing the dates."

        districts = json.loads(response.text)
        district_names = districts.values()
        dist_names = list(district_names)
        for dist in dist_names[1:]:
            Fertilizermisscrapper.final_table = Fertilizermisscrapper.final_table.iloc[0:0]
            Fertilizermisscrapper.page_number = 1
            current= datetime(2018,1,1)
            start_date= datetime(2017,1,1)
            for i in list(rrule(MONTHLY, dtstart=start_date, until=current)):
                j = i + relativedelta(months = 1)
                yield Request(
                "https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action?parameterFromDate={}%2F{}%2F{}&parameterDistrictName={}&d-6849390-p=1&parameterStateName={}&parameterToDate={}%2F{}%2F{}".format(
                    i.strftime("%d"),
                    i.strftime("%m"),
                    i.strftime("%Y"),
                    dist,
                    response.meta["State"],
                    j.strftime("%d"),
                    j.strftime("%m"),
                    j.strftime("%Y")),
                    method="Post",
                    meta={
                    "State": response.meta["State"],
                    "District": dist,
                    "From Date": i.strftime("%d/%m/%Y"),
                    "To Date": j.strftime("%d/%m/%Y"),
                    "From Year": i.strftime('%Y'),
                    "From month":i.strftime('%m'),
                    'From day': i.strftime('%d'),
                    "To Year": j.strftime('%Y'),
                    "To month": j.strftime('%m'),
                    "To Day": j.strftime('%d'),
                     },
                    callback=self.save_csv
            )
    def save_csv(self, response):
            "This fuction saves the csv file in the defined path."
            meta_data = dict(response.meta)
            month_number = meta_data.get("From month")
            month = calendar.month_name[int(month_number)]
            try:
                table = response.css('#districtTable').get()
                table_list = pd.read_html(table)
                data = table_list[0]
                data_len = len(data)
                Fertilizermisscrapper.final_table = pd.concat([Fertilizermisscrapper.final_table,data], sort = False)
                print(Fertilizermisscrapper.final_table)

                if data.equals(Fertilizermisscrapper.final_table[data_len-1:])==False:
                    Fertilizermisscrapper.page_number += 1
                    yield Request("https://reports.dbtfert.nic.in/mfmsReports/getPOSReportFormList.action?parameterFromDate={}%2F{}%2F{}&parameterDistrictName={}&d-6849390-p={}&parameterStateName={}&parameterToDate={}%2F{}%2F{}".format(
                    meta_data.get('From day'),
                    meta_data.get('From month'),
                    meta_data.get('From Year'),
                    meta_data.get('District'),
                    Fertilizermisscrapper.page_number,
                    meta_data.get('State'),
                    meta_data.get('To Day'),
                    meta_data.get('To month'),
                    meta_data.get('To Year'),
                ), callback= self.save_csv)
                else:
                    Fertilizermisscrapper.final_table = Fertilizermisscrapper.final_table.iloc[0:data_len]

                file_path = self.parent_folder + "/" + meta_data.get('State') + "/" + meta_data.get('District')
                file_name = meta_data.get('From Year') + '_' + month + '.csv'
                self.directory(file_path)
                Fertilizermisscrapper.final_table.to_csv(file_path + '/' + file_name)





    def directory(self, file_path):
        path_parts = file_path.split('/')
        for i in range(1, len(path_parts) + 1):
            present_path = '/'.join(path_parts[:i])
            Path(present_path).mkdir(exist_ok=True)

def main():

    process = CrawlerProcess()
    process.crawl(Fertilizermisscrapper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
