# import datetime
import datetime
import json
from datetime import timedelta
from pathlib import Path

import pandas as pd
import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# creating data directories, if not present in data/raw folder

project_dir = str(Path(__file__).resolve().parents[2])
raw_folder = project_dir + "/data/raw/"
imd_raw_folder = project_dir + "/data/raw/"
imd_raw_folder_path = Path(imd_raw_folder)
imd_raw_folder_path.mkdir(parents=True, exist_ok=True)


def daterange(start_date, end_date):
    for n in range((end_date - start_date).days):
        yield start_date + timedelta(n)


class groundwaterdata(scrapy.Spider):
    name = "groundwaterdata"
    dataset_name = "gw"
    cluster_type = "idp"
    project_dir = str(Path(__file__).resolve().parents[3])
    raw_folder = project_dir + "/data/raw/"
    cluster_type = "idp"

    # print(raw_folder)

    def __init__(self):
        print("NOTE: Enter yyyy-mm-dd format for only current year data")
        self.start_date_input = input(
            "Enter the start year to download the data in yyyy-mm-dd format:"
        )
        self.end_date_input = input("Enter the end year in yyyy-mm-dd format:")

    def start_requests(self):
        """
        This will initiate the requests
        """
        yield Request("https://wdo.indiawris.gov.in/api/comm/src/groundwater")

    def parse(self, response):

        source_list = json.loads(response.text)
        imd_dict = source_list[0]
        # getting the location
        loc_uuid = imd_dict["locUUID"]
        # logic to iterate over each day between the mentioned dates
        end_date = datetime.datetime.strptime(
            self.end_date_input, "%Y-%m-%d"
        ) + timedelta(1)
        start_date = datetime.datetime.strptime(self.start_date_input, "%Y-%m-%d")
        for singledate in daterange(start_date, end_date):
            # st_date = datetime.datetime.strftime(singledate, "%Y-%m-%d")
            # en_date = datetime.datetime.strftime((singledate), "%Y-%m-%d")
            sDate = datetime.datetime.strftime(singledate, "%Y-%m-%d").replace("-", "")
            # eDate = datetime.datetime.strftime((singledate), "%Y-%m-%d").replace("-", "")
            # print("===========**===========")
            # print(sDate)
            # print(eDate)
            rDate = singledate.strftime("%d-%m-%Y").replace("-", "")
            print(rDate)

            y = singledate.year
            m = singledate.strftime("%B")
            state_filename = (
                "state_" + singledate.strftime("%d-%m-%Y").replace("-", "") + ".csv"
            )

            file_path_state = (
                imd_raw_folder
                + "/"
                + "state_level"
                + "/"
                + str(y)
                + "/"
                + str(m)
                + "/"
                + state_filename
            )
            print(file_path_state)

            if Path(file_path_state).is_file():
                print("YYEEESSS")
                pass
            else:
                print("NOOOOOOO")
                state_payload = {
                    "cType": "STATE",
                    "component": "groundwater",
                    "eDate": sDate,
                    "format": "yyyyMMdd",
                    "lType": "COUNTRY",
                    "lUUID": loc_uuid,
                    "locname": "INDIA",
                    "loctype": "COUNTRY",
                    "locuuid": loc_uuid,
                    "mapOnClickParams": "false",
                    "pUUID": loc_uuid,
                    "parentLocName": "INDIA",
                    "sDate": sDate,
                    "seasonYear": "2018",
                    "src": "STATE_AND_CENTRAL_STATION",
                    "summary": "false",
                    "telementryfilter": "All",
                    "type": "Depth to water level (DTW)",
                    "view": "ADMIN",
                    "ytd": "2021",
                }
                state_payload = json.dumps(state_payload)
                headers = {
                    "Content-Type": "application/json"
                    # "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                }

                yield Request(
                    url="https://wdo.indiawris.gov.in/api/gw/gwTable",
                    method="POST",
                    callback=self.state_parser,
                    body=state_payload,
                    headers=headers,
                    meta={"start_date": singledate, "sDate": sDate},
                )

    def state_parser(self, response):
        state_data_daily = pd.DataFrame(json.loads(response.text))
        state_data_daily["date"] = response.meta["start_date"].strftime("%d-%m-%Y")
        state_data_daily = state_data_daily.rename(columns={"name": "state_name"})
        # print(state_data_daily)
        year = response.meta["start_date"].year
        month_name = response.meta["start_date"].strftime("%B")
        # print(
        #     "Creating DataFrame at state level on ",
        #     response.meta["start_date"],
        # )

        state_level_path = (
            imd_raw_folder
            + "/"
            + "state_level"
            + "/"
            + str(year)
            + "/"
            + str(month_name)
        )

        path = Path(state_level_path)

        path.mkdir(parents=True, exist_ok=True)

        state_level_filename = (
            "state_"
            + response.meta["start_date"].strftime("%d-%m-%Y").replace("-", "")
            + ".csv"
        )
        # writing the state_level daily data file.
        state_data_daily.to_csv(str(path) + "/" + state_level_filename, index=False)

        # upload to sharepoint folders.
        # 1. Ensure folders
        # 2. Upload file
        # target_folder_path = (
        #     "raw/" + "state_level" + "/" + str(year) + "/" + str(month_name)
        # )
        # dataset_name = self.dataset_name
        # cluster_type = self.cluster_type
        # Ensure Folders
        # ensure_folders(target_folder_path, dataset_name, cluster_type)
        # # Upload File
        # source_file_path = str(path) + "/" + state_level_filename
        # remote_file_name = state_level_filename
        # upload_file(
        #     source_file_path,
        #     target_folder_path,
        #     remote_file_name,
        #     dataset_name,
        #     cluster_type,
        # )

        sDate = response.meta["sDate"]

        for i, row in state_data_daily.iterrows():
            state_name = row["state_name"]
            state_loc_id = row["uuid"]

            y = response.meta["start_date"].year
            m = response.meta["start_date"].strftime("%B")
            district_filename = (
                state_name.lower()
                + "_"
                + response.meta["start_date"].strftime("%d-%m-%Y").replace("-", "")
                + ".csv"
            )

            file_path_dis = (
                imd_raw_folder
                + "/"
                + "district_level"
                + "/"
                + state_name
                + "/"
                + str(y)
                + "/"
                + str(m)
                + "/"
                + district_filename
            )
            print(file_path_dis)

            if Path(file_path_dis).is_file():
                print("YYEEESSS")
                # pass
            else:
                print("NOOOOOO")
                district_payload = {
                    "cType": "DISTRICT",
                    "component": "groundwater",
                    "eDate": sDate,
                    "format": "yyyyMMdd",
                    "lType": "STATE",
                    "lUUID": state_loc_id,
                    "locname": state_name,
                    "loctype": "STATE",
                    "locuuid": state_loc_id,
                    "mapOnClickParams": "true",
                    "pUUID": state_loc_id,
                    "parentLocName": "INDIA",
                    "sDate": sDate,
                    "seasonYear": "2018",
                    "src": "STATE_AND_CENTRAL_STATION",
                    "summary": "false",
                    "telementryfilter": "All",
                    "type": "Depth to water level (DTW)",
                    "view": "ADMIN",
                    "ytd": "2021",
                }
                district_payload = json.dumps(district_payload)
                headers = {
                    "Content-Type": "application/json"
                    # "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                }
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/gw/gwTable",
                    method="POST",
                    callback=self.district_parser,
                    body=district_payload,
                    meta={
                        "state_name": state_name,
                        "state_loc_id": state_loc_id,
                        "sDate": sDate,
                        "start_date": response.meta["start_date"],
                    },
                    headers=headers,
                )

    def district_parser(self, response):
        district_data_daily = pd.DataFrame(json.loads(response.text))
        district_data_daily["date"] = response.meta["start_date"].strftime("%d-%m-%Y")
        district_data_daily["state_name"] = response.meta["state_name"]
        district_data_daily = district_data_daily.rename(
            columns={"name": "district_name"}
        )
        # print(district_data_daily)
        year = response.meta["start_date"].year
        month_name = response.meta["start_date"].strftime("%B")
        # print(
        #     "Creating DataFrame at state level on ",
        #     response.meta["start_date"],
        # )

        distritct_level_path = (
            imd_raw_folder
            + "/"
            + "district_level"
            + "/"
            + response.meta["state_name"]
            + "/"
            + str(year)
            + "/"
            + str(month_name)
        )

        path = Path(distritct_level_path)

        path.mkdir(parents=True, exist_ok=True)

        district_level_filename = (
            response.meta["state_name"].lower()
            + "_"
            + response.meta["start_date"].strftime("%d-%m-%Y").replace("-", "")
            + ".csv"
        )
        # writing the state_level daily data file.
        district_data_daily.to_csv(
            str(path) + "/" + district_level_filename, index=False
        )
        # upload to sharepoint folders.
        # 1. Ensure folders
        # 2. Upload file
        # target_folder_path = (
        #     "raw/"
        #     + "distrtict_level"
        #     + "/"
        #     + response.meta["state_name"]
        #     + "/"
        #     + str(year)
        #     + "/"
        #     + str(month_name)
        # )
        # dataset_name = self.dataset_name
        # cluster_type = self.cluster_type
        # Ensure Folders
        # ensure_folders(target_folder_path, dataset_name, cluster_type)
        # Upload File
        # source_file_path = str(path) + "/" + district_level_filename
        # remote_file_name = district_level_filename
        # # upload_file(
        #     source_file_path,
        #     target_folder_path,
        #     remote_file_name,
        #     dataset_name,
        #     cluster_type,
        # )

        sDate = response.meta["sDate"]

        for i, row in district_data_daily.iterrows():
            district_name = row["district_name"]
            district_loc_id = row["uuid"]

            y = response.meta["start_date"].year
            m = response.meta["start_date"].strftime("%B")
            station_filename = (
                response.meta["state_name"].lower()
                + "_"
                + district_name.lower()
                + "_"
                + response.meta["start_date"].strftime("%d-%m-%Y").replace("-", "")
                + ".csv"
            )

            file_path_station = (
                imd_raw_folder
                + "/"
                + "station_level"
                + "/"
                + response.meta["state_name"]
                + "/"
                + district_name
                + "/"
                + str(y)
                + "/"
                + str(m)
                + "/"
                + station_filename
            )
            print(file_path_station)

            if Path(file_path_station).is_file():
                print("YYEEESSS")
                pass
            else:
                print("NOOOOO")
                print(sDate)
                station_payload = {
                    "cType": "STATION",
                    "component": "groundwater",
                    "eDate": sDate,
                    "format": "yyyyMMdd",
                    "lType": "DISTRICT",
                    "lUUID": district_loc_id,
                    "locname": district_name,
                    "loctype": "DISTRICT",
                    "locuuid": district_loc_id,
                    "mapOnClickParams": "true",
                    "pUUID": district_loc_id,
                    "parentLocName": "INDIA",
                    "sDate": sDate,
                    "seasonYear": "2018",
                    "src": "STATE_AND_CENTRAL_STATION",
                    "summary": "false",
                    "telementryfilter": "All",
                    "type": "Depth to water level (DTW)",
                    "view": "ADMIN",
                    "ytd": "2021",
                }
                station_payload = json.dumps(station_payload)
                headers = {
                    "Content-Type": "application/json"
                    # "Referer": "https://wdo.indiawris.gov.in/waterdataonline/analysis;selectedsidebar=downloadreport;component=All",
                }
                yield Request(
                    url="https://wdo.indiawris.gov.in/api/gw/gwTable",
                    method="POST",
                    callback=self.station_parser,
                    body=station_payload,
                    meta={
                        "district_name": district_name,
                        "state_name": response.meta["state_name"],
                        "district_loc_id": district_loc_id,
                        "sDate": sDate,
                        "start_date": response.meta["start_date"],
                    },
                    headers=headers,
                )

    def station_parser(self, response):
        station_data_daily = pd.DataFrame(json.loads(response.text))
        station_data_daily["date"] = response.meta["start_date"].strftime("%d-%m-%Y")

        station_data_daily["district_name"] = response.meta["district_name"]
        station_data_daily["state_name"] = response.meta["state_name"]
        station_data_daily = station_data_daily.rename(columns={"name": "station_name"})
        # print(station_data_daily)
        year = response.meta["start_date"].year
        month_name = response.meta["start_date"].strftime("%B")
        # print(
        #     "Creating DataFrame at state level on ",
        #     response.meta["start_date"],
        # )

        station_level_path = (
            imd_raw_folder
            + "/"
            + "station_level"
            + "/"
            + response.meta["state_name"]
            + "/"
            + response.meta["district_name"]
            + "/"
            + str(year)
            + "/"
            + str(month_name)
        )

        path = Path(station_level_path)

        path.mkdir(parents=True, exist_ok=True)

        station_level_filename = (
            response.meta["state_name"].lower()
            + "_"
            + response.meta["district_name"].lower()
            + "_"
            + response.meta["start_date"].strftime("%d-%m-%Y").replace("-", "")
            + ".csv"
        )
        # writing the state_level daily data file.
        station_data_daily.to_csv(str(path) + "/" + station_level_filename, index=False)
        # upload to sharepoint folders.
        # 1. Ensure folders
        # 2. Upload file
        # target_folder_path = (
        #     "raw/"
        #     + "station_level"
        #     + "/"
        #     + response.meta["district_name"]
        #     + "/"
        #     + str(year)
        #     + "/"
        #     + str(month_name)
        # )
        # dataset_name = self.dataset_name
        # cluster_type = self.cluster_type
        # Ensure Folders
        # ensure_folders(target_folder_path, dataset_name, cluster_type)
        # Upload File
        # source_file_path = str(path) + "/" + station_level_filename
        # remote_file_name = station_level_filename
        # upload_file(
        #     source_file_path,
        #     target_folder_path,
        #     remote_file_name,
        #     dataset_name,
        #     cluster_type,
        # )

        pass


def main():

    settings = get_project_settings()
    settings.set("CUSTOM_SETTING", "Super Custom Setting")
    settings.update(
        {
            "CONCURRENT_REQUESTS": 10,
            "ROBOTSTXT_OBEY": True,
            # "AUTOTHROTTLE_ENABLED": True,
            # "DOWNLOAD_DELAY": 1.5,
            "BOT_NAME": "gwbot",
            # "DOWNLOADER_MIDDLEWARES": {
            #     "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": 390,
            #     "scrapy.downloadermiddlewares.retry.RetryMiddleware": 391,
            #     "scrapy_fake_useragent.middleware.RandomUserAgentMiddleware": 400,
            #     "scrapy_fake_useragent.middleware.RetryUserAgentMiddleware": 401,
            # },
        }
    )
    process = CrawlerProcess(settings)
    process.crawl(groundwaterdata)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
