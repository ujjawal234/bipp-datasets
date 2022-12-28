import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class psdataMaharshtrascraper(scrapy.Spider):
    name = "psdataMaharashtra"

    custom_settings = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    }

    # project_dir = str(Path(__file__).resolve().parents[3])
    # parent_folder = project_dir + "/data/raw/"

    def start_requests(self):
        # request to initiate the scraping
        yield Request("https://ceo.maharashtra.gov.in/Lists/ListPSs.aspx")

    def parse(self, response):
        # "This fuction will parse the names of all the districts in the state and will raise another request to get all ACS in the district."

        dist_names = response.xpath(
            '//select[@id="mainContent_DistrictList"]/option/text()'
        ).extract()
        dist_values = response.xpath(
            '//select[@id="mainContent_DistrictList"]/option/@value'
        ).extract()
        print(dist_names)
        # print(dist_values)
        for dist in dist_values[0:]:
            yield FormRequest.from_response(
                response,
                url="https://ceo.maharashtra.gov.in/Lists/ListPSs.aspx",
                method="POST",
                formdata={"ctl00$mainContent$DistrictList": dist},
                dont_click="True",
                meta={"district_code": dist},
                callback=self.ac_data,
            )

    def ac_data(self, response):

        ac_names = response.xpath(
            '//select[@id="mainContent_AssemblyList"]/option/text()'
        ).extract()
        ac_values = response.xpath(
            '//select[@id="mainContent_AssemblyList"]/option/@value'
        ).extract()
        form_dict1 = {
            "__EVENTTARGET": "ctl00$mainContent$AssemblyList",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract(),
            "ctl00$mainContent$DistrictList": response.meta["district_code"],
            "ctl00$mainContent$AssemblyList": "",
            "ctl00$mainContent$LangList": "2",
        }
        i = 1
        for ac in ac_values[1:]:
            form_dict1["ctl00$mainContent$AssemblyList"] = ac
            yield FormRequest.from_response(
                response,
                url="https://ceo.maharashtra.gov.in/Lists/ListPSs.aspx",
                method="POST",
                formdata=form_dict1,
                # dont_click="True",
                meta={
                    "ac_names": ac_names[i],
                    "dist_code": response.meta["district_code"],
                },
                callback=self.ps_newresponse,
            )
            i += 1
            print(form_dict1)

    def ps_newresponse(self, response):

        ac_names = response.xpath(
            '//select[@id="mainContent_AssemblyList"]/option/text()'
        ).extract()
        ac_values = response.xpath(
            '//select[@id="mainContent_AssemblyList"]/option/@value'
        ).extract()

        i = 1
        form_dict = {
            "ctl00$WebScriptManager": "ctl00$WebScriptManager|ctl00$mainContent$ReportViewer1$ctl09$Reserved_AsyncLoadTarget",
            " __EVENTTARGET": "ctl00$mainContent$ReportViewer1$ctl09$Reserved_AsyncLoadTarget",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract(),
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract(),
            "ctl00$mainContent$DistrictList": "",
            "ctl00$mainContent$AssemblyList": "",
            "ctl00$mainContent$LangList": "2",
            "ctl00$mainContent$ReportViewer1$ctl03$ctl00": "",
            "ctl00$mainContent$ReportViewer1$ctl03$ctl01": "",
            "ctl00$mainContent$ReportViewer1$ctl10": "ltr",
            "ctl00$mainContent$ReportViewer1$ctl11": "standards",
            "ctl00$mainContent$ReportViewer1$AsyncWait$HiddenCancelField": "False",
            "ctl00$mainContent$ReportViewer1$ToggleParam$store": "",
            "ctl00$mainContent$ReportViewer1$ToggleParam$collapse": "false",
            "ctl00$mainContent$ReportViewer1$ctl05$ctl00$CurrentPage": "",
            "ctl00$mainContent$ReportViewer1$ctl05$ctl03$ctl00": "",
            "ctl00$mainContent$ReportViewer1$ctl08$ClientClickedId": "",
            "ctl00$mainContent$ReportViewer1$ctl07$store": "",
            "ctl00$mainContent$ReportViewer1$ctl07$collapse": "false",
            "ctl00$mainContent$ReportViewer1$ctl09$VisibilityState$ctl00": "None",
            "ctl00$mainContent$ReportViewer1$ctl09$ScrollPosition": "",
            "ctl00$mainContent$ReportViewer1$ctl09$ReportControl$ctl02": "",
            "ctl00$mainContent$ReportViewer1$ctl09$ReportControl$ctl03": "",
            "ctl00$mainContent$ReportViewer1$ctl09$ReportControl$ctl04": "100",
            "__ASYNCPOST": "true",
        }
        for ac in ac_values[1:]:
            form_dict["ctl00$mainContent$DistrictList"] = response.meta[
                "dist_code"
            ]
            form_dict["ctl00$mainContent$AssemblyList"] = ac
            yield FormRequest.from_response(
                response,
                url="https://ceo.maharashtra.gov.in/Lists/ListPSs.aspx",
                method="POST",
                formdata=form_dict,
                # dont_click="True",
                meta={"ac_names": ac_names[i]},
                callback=self.save_data,
            )
            i += 1

    def save_data(self, response):
        pass
        # print(response.text)
        # table_list= pd.read_html(response.text)
        # print(table_list)


def main():
    process = CrawlerProcess()
    # process = CrawlerProcess({
    # 'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36'})
    process.crawl(psdataMaharshtrascraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
