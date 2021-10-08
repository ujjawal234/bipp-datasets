# import datetime
import json
from datetime import date, timedelta
from pathlib import Path

# import pandas as pd
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess


class RainfallWris(scrapy.Spider):
    name = "rainfallwris"
    dataset_name = "Rainfall"
    state_data = list()
    district_data = list()
    station_data = list()
    project_dir = str(Path(__file__).resolve().parents[3])
    parent_folder = project_dir + "/data/raw/"
    print(parent_folder)

    def __init__(self, input="inputargument", start_year=None, end_year=None):
        self.input = input  # source file name
        self.star_year_input = start_year
        self.end_year_input = end_year

    def start_requests(self):
        # this is the request that will initiate the scraping the data
        yield Request(
            "https://wdo.indiawris.gov.in/api/comm/src/rainfall_report"
        )

    def parse(self, response):

        source_list = json.loads(response.text)

        for source in source_list:
            if source["value"] == "ALL AGENCIES":
                # Getting the vallues for all the states
                state_payload = {
                    "pType": "COUNTRY",
                    "cType": "STATE",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                district_payload = {
                    "pType": "STATE",
                    "cType": "DISTRICT",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                station_payload = {
                    "pType": "DISTRICT",
                    "cType": "STATION",
                    "component": "rainfall",
                    "src": "STATE_AND_CENTRAL_STATION",
                }
                state_values_payload = json.dumps(state_payload)
                district_values_payload = json.dumps(district_payload)
                station_values_payload = json.dumps(station_payload)

                headers = {
                    "Content-Type": "application/json",
                    "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                }
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.state_parser,
                    body=state_values_payload,
                    headers=headers,
                )
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.district_parser,
                    body=district_values_payload,
                    headers=headers,
                )
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/locations/allChildrenForParentChildType",
                    method="POST",
                    callback=self.station_parser,
                    body=station_values_payload,
                    headers=headers,
                )

    def state_parser(self, response):
        # print(response.text)
        print("State Data")
        self.state_data = json.loads(response.text)
        # print(self.state_data)

    def district_parser(self, response):
        # print(response.text)
        self.district_data = json.loads(response.text)
        print("District Data")
        # print(self.district_data)

    def station_parser(self, response):
        # print(response.text)
        self.station_data = json.loads(response.text)
        print("Station Data")
        # print(self.station_data)
        # print(self.state_data["INDIA"])
        # print(self.station_data)
        for state in self.state_data["INDIA"]:
            print("State %s", state)
            state_name = state["name"]
            state_uid = state["uuid"]
            for district_entity in self.district_data[state["name"]]:
                district_name = district_entity["name"]
                district_uid = district_entity["uuid"]
                try:
                    for station_entity in self.station_data[district_name]:
                        print("Station " * 5)
                        print(station_entity["name"])
                        print(station_entity["uuid"])
                        print(station_entity["sc"])
                        station_name = station_entity["name"]
                        # station_sc = station_entity["sc"]
                        station_uid = station_entity["uuid"]

                        headers = {
                            "Content-Type": "application/json",
                            "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                        }

                        today_date = date.today()
                        if (
                            self.start_year_input & self.end_year_input
                            == today_date.year
                        ):
                            start_date_value = input(
                                "Enter the required date in yyyymmdd format"
                            )
                            # taking the previous day's date as end date
                            end_date_value = (
                                today_date - timedelta(days=1)
                            ).strftime("yyyymmdd")
                            print("Given Start Date is , %s", start_date_value)
                            print("Given End Date is , %s", end_date_value)
                            station_complete_values = {
                                "lType": "STATION",
                                "src": "STATE_AND_CENTRAL_STATION",
                                "view": "ADMIN",
                                "aggr": "SUM",
                                "reportType": "Station Wise Timeseries",
                                "sDate": start_date_value,
                                "eDate": end_date_value,
                                "fileformat": "xls",
                                "calendarFormat": "yyyyMMdd",
                                "STATE": [state_uid],
                                "DISTRICT": [district_uid],
                                "STATION": [station_uid],
                                "stationType": "ALL",
                            }
                            station_complete_payload = json.dumps(
                                station_complete_values
                            )
                            # yielding a request to download the file.
                            yield Request(
                                url="https://wdo.indiawris.gov.in/api/rf/report",
                                method="POST",
                                callback=self.file_downloader,
                                body=station_complete_payload,
                                headers=headers,
                                meta={
                                    "state_name": state_name,
                                    "district_name": district_name,
                                    "station_name": station_name,
                                    "start_year": station_complete_values[
                                        "sDate"
                                    ],
                                    "end_year": station_complete_values[
                                        "eDate"
                                    ],
                                },
                            )

                        else:
                            # this condition is to get historical data until 2020.
                            for year in range(
                                self.start_year_input, self.end_year_input + 1
                            ):

                                start_date_value = str(year) + "0101"
                                end_date_value = str(year) + "1231"
                                station_complete_values = {
                                    "lType": "STATION",
                                    "src": "STATE_AND_CENTRAL_STATION",
                                    "view": "ADMIN",
                                    "aggr": "SUM",
                                    "reportType": "Station Wise Timeseries",
                                    "sDate": start_date_value,
                                    "eDate": end_date_value,
                                    "fileformat": "xls",
                                    "calendarFormat": "yyyyMMdd",
                                    "STATE": [state_uid],
                                    "DISTRICT": [district_uid],
                                    "STATION": [station_uid],
                                    "stationType": "ALL",
                                }
                                station_complete_payload = json.dumps(
                                    station_complete_values
                                )
                                # yielding a request to download the file.
                                yield Request(
                                    url="https://wdo.indiawris.gov.in/api/rf/report",
                                    method="POST",
                                    callback=self.file_downloader,
                                    body=station_complete_payload,
                                    headers=headers,
                                    meta={
                                        "state_name": state_name,
                                        "district_name": district_name,
                                        "station_name": station_name,
                                        "start_year": station_complete_values[
                                            "sDate"
                                        ],
                                        "end_year": station_complete_values[
                                            "eDate"
                                        ],
                                    },
                                )

                except Exception as err:
                    print(err)
                    print(
                        "ERROR: district not found %d, %d",
                        district_name,
                        state_name,
                    )
                    print("No Stations Available in this District")
                    # Write a logic to note down all the districts where there are no stations available.
        pass

    def file_downloader(self, response):

        path = Path(
            self.parent_folder
            + "/"
            + response.meta["state_name"]
            + "/"
            + response.meta["district_name"]
            + "/"
            + response.meta["start_year"]
            + "_"
            + response.meta["end_year"]
            + "/"
        )

        path.mkdir(parents=True, exist_ok=True)
        # unquote() cleans up the reserved characters in the url to create a clean file name
        filename = (
            str(path)
            + "/"
            + response.meta["start_year"]
            + "_"
            + response.meta["end_year"]
            + ".xls"
        )

        with open(filename, "wb") as f:
            f.write(response.body)


def main():
    start_year_input = input(
        "Enter the start year to download the data in yyyy format"
    )
    end_year_input = input("Enter the end year in yyyy format")
    process = CrawlerProcess()
    process.crawl(
        RainfallWris,
        input="inputargument",
        start_year=start_year_input,
        end_year=end_year_input,
    )
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
