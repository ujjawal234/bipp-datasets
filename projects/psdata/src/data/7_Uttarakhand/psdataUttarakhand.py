# from pathlib import Path
import pandas as pd
import scrapy
from scrapy import FormRequest, Request

# from scrapy.selector import Selector
# from googletrans import Translator
from scrapy.crawler import CrawlerProcess


class psdataUttarakhandscraper(scrapy.Spider):
    name = "psdataUttarakhand"

    # project_dir = str(Path(__file__).resolve().parents[3])
    # parent_folder = project_dir + "/data/raw/"

    def start_requests(self):

        # request to initiate the scraping
        yield Request(
            "http://election.uk.gov.in/", method="POST", callback=self.roll_parser
        )

    def roll_parser(self, response):
        form_dict = {
            "__EVENTTARGET": "ddlRollDesc",
            "__EVENTARGUMENT": response.css(
                "#__EVENTARGUMENT::attr(value)"
            ).extract_first(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract_first(),
            "__VIEWSTATE": "/wEPDwUKMjEzNzc2NzA4MQ8WAh4TVmFsaWRhdGVSZXF1ZXN0TW9kZQIBFgICAw9kFhACBQ8QDxYGHg5EYXRhVmFsdWVGaWVsZAUCSUQeDURhdGFUZXh0RmllbGQFCFJvbGxEZXNjHgtfIURhdGFCb3VuZGdkEBUSDC0tIFNlbGVjdCAtLTNGaW5hbCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDIxIChNT1RIRVIgUk9MTCk3RmluYWwgRWxlY3RvcmFsIFJvbGwgQXMgT24gMDEtMDEtMjAyMSAoU1VQUExFTUVOVEFSWS1JKT5EcmFmdCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDIxIChQVUJMSVNIRUQgLSAxNi0xMS0yMDIwKTNGaW5hbCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDIwIChNT1RIRVIgUk9MTCk3RmluYWwgRWxlY3RvcmFsIFJvbGwgQXMgT24gMDEtMDEtMjAyMCAoU1VQUExFTUVOVEFSWS1JKT5EcmFmdCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDIwIChQVUJMSVNIRUQgLSAxNi0xMi0yMDE5KTRGaW5hbCAgRWxlY3RvcmFsIFJvbGwgQXMgT24gMjUtMDMtMjAxOSAoTU9USEVSIFJPTEwpOEZpbmFsICBFbGVjdG9yYWwgUm9sbCBBcyBPbiAyNS0wMy0yMDE5IChTVVBQTEVNRU5UQVJZLUkpOUZpbmFsICBFbGVjdG9yYWwgUm9sbCBBcyBPbiAyNS0wMy0yMDE5IChTVVBQLUlJIEFERElUSU9OKUhGaW5hbCAgRWxlY3RvcmFsIFJvbGwgQXMgT24gMjUtMDMtMjAxOSAoU1VQUC1JSSBERUxFVElPTiAmIE1PRElGSUNBVElPTikuRmluYWwgU2VydmljZSAgRWxlY3RvcmFsIFJvbGwgQXMgT24gMDEtMDEtMjAxOTNGaW5hbCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDE5IChNT1RIRVIgUk9MTCk3RmluYWwgRWxlY3RvcmFsIFJvbGwgQXMgT24gMDEtMDEtMjAxOSAoU1VQUExFTUVOVEFSWS1JKS5EcmFmdCBTZXJ2aWNlICBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDE5JURyYWZ0IEVsZWN0b3JhbCBSb2xsIEFzIE9uIDAxLTAxLTIwMTklRmluYWwgRWxlY3RvcmFsIFJvbGwgQXMgT24gMDEtMDEtMjAxOCVEcmFmdCBFbGVjdG9yYWwgUm9sbCBBcyBPbiAwMS0wMS0yMDE4FRICLTECMTcCMTYCMTUCMTQCMTMCMTICMTECMTABOQE4ATcBNgE1ATQBMwEyATEUKwMSZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnFgECAWQCBw8PFgIeB1Zpc2libGVnZGQCCQ8QDxYIHwRnHwEFC0Rpc3RyaWN0X0lEHwIFEERpc3RyaWN0X05hbWVfRW4fA2dkEBUODC0tIFNlbGVjdCAtLR0wMSAtIOCkueCksOCkv+CkpuCljeCkteCkvuCksBowMiAtIOCkqOCliOCkqOClgOCkpOCkvuCksh0wMyAtIOCkheCksuCljeKAjeCkruCli+CkoeCkviMwNCAtIOCkiuCkp+CkruCkuOCkv+CkguCkueCkqOCkl+CksB0wNSAtIOCkquCkv+CkpeCljOCksOCkvuCkl+CkoiAwNiAtIOCkrOCkvuCkl+Clh+CktuCljeKAjeCkteCksB0wNyAtIOCkmuCkruCljeKAjeCkquCkvuCkteCkpBQwOCAtIOCkmuCkruCli+CksuClgCMwOSAtIOCkieCkpOCljeKAjeCkpOCksOCkleCkvuCktuClgCYxMCAtIOCksOClgeCkpuCljeCksOCkquCljeCksOCkr+CkvuCklyQxMSAtIOCkn+Ckv+CkueCksOClgCDgpJfgpKLgpLXgpL7gpLIhMTIgLSDgpKrgpYzgpKHgpYAg4KSX4KSi4KS14KS+4KSyHTEzIC0g4KSm4KWH4KS54KSw4KS+4KSm4KWC4KSoFQ4CLTECMDECMDICMDMCMDQCMDUCMDYCMDcCMDgCMDkCMTACMTECMTICMTMUKwMOZ2dnZ2dnZ2dnZ2dnZ2cWAQIBZAILDw8WAh8EZ2RkAg0PEA8WCB8EZx8BBQVBQ19OTx8CBQpBQ19OQU1FX0VOHwNnZBAVDAwtLSBTZWxlY3QgLS0dMjUgLSDgpLngpLDgpL/gpKbgpY3gpLXgpL7gpLAwMjYgLSDgpKzgpYDgpI/gpJrgpIjgpI/gpLIg4KSw4KS+4KSo4KWA4KSq4KWB4KSwIzI3IC0g4KSc4KWN4oCN4KS14KS+4KSy4KS+4KSq4KWB4KSwHTI4IC0g4KSt4KSX4KS14KS+4KSo4KSq4KWB4KSwGjI5IC0g4KSd4KSs4KSw4KWH4KSh4KS84KS+JDMwIC0g4KSq4KS/4KSw4KS+4KSoIOCkleCksuCkv+Ckr+CksBczMSAtIOCksOClgeCkoeCkvOCkleClgBczMiAtIOCkluCkvuCkqOCkquClgeCksBczMyAtIOCkruCkguCkl+CksuCljOCksBczNCAtIOCksuCkleCljeKAjeCkuOCksDMzNSAtIOCkueCksOCkv+CkpuCljeCkteCkvuCksCDgpJfgpY3gpLDgpL7gpK7gpYDgpKMVDAItMQIyNQIyNgIyNwIyOAIyOQIzMAIzMQIzMgIzMwIzNAIzNRQrAwxnZ2dnZ2dnZ2dnZ2cWAWZkAg8PDxYCHwRnZGQCEQ8QDxYIHwRnHwEFB1BBUlRfTk8fAgUMUEFSVF9OQU1FX0VOHwNnZBAVAQwtLSBTZWxlY3QgLS0VAQItMRQrAwFnFgFmZAITDw8WAh8EaGRkGAEFCENhcHRjaGExDwUkODZkY2U0MjctYWNhMi00OTg2LWI1MDYtNDNlNTQyMzBkODhjZPJp14KW9i6Mv3gWzArglDipfu9csiNkkssbXqAttVNZ",
            "__VIEWSTATEGENERATOR": "CA0B0334",
            "__EVENTVALIDATION": "/wEdADJzTJq5Nyqsbmcf8c5MU7nD4sluggLnBgl1Cuwv/Bk2jyuW3qQ4HgE2iyZVSDhrYf/ZohXB9giat2VwHp2UFnKD2FrOpZvBj1R40avbDYaVzGdn3+Q/fxgeeWIJzkCekzbkAxMZsLwlACSrEJlczVyRyQToXPiwC3bB7TUBDQ0DbN28ByfUj3yuYUYPmVGGICB+gxgwMwMfvQJVsAmofF7bXrLlpKNXA5cXjDXg38yvkBxHoqjUJ6YgR93bTj0AwlrTuDbTndOuELokO9Uh6bKyulZdlJ7eOnBzNwj/Q5fDJrZo63GQDy0cGQV1jEJF1iBOvw7HHtqknJ88X7dlJHlrK7euzJA3mlulxb5iV8prjEopfoP4jujT+pM2zAhw5M/YZ0ggZU/DQbVnukQHkaWUOGMB6khlbaLTG/ADkoxkA0nwr5X6FZS+PDXERdsH/VblMi5KogPtz8SZ4VOOKN38yPs2MuTwTrKiYW0/jWiz0w3h1bLqLjnEN2yImsbZRx2yp6LI3eGqLlq0DWZD7IemOCEeqnrRQPdCJ7fTF7IYDOLYaNlbzjBpwUKWcuy5mClFG7TjuMs/UjlC+z5ml+/2znQIQZCxB1nqGh4DozhNuU65aNWJeD/+go/9kDi1MU8OqW8QKAD7PQ5sKqq4czMMYIl52YEPVI/VEKHBBt0yQ5EEGoF9S+VScepqOskuqHQS0I0RBiyifFYja2//0VM0drnj9pejOrcGvN1jmU4bizA9Xzd5L9AatbN9K4LJqjPC2pvDWZQyfhDYhUcV7+S39QpLMe9pUmQTP/gFZirRATMMDMrVzu6bcYviMnkC0sJTXvKwvS2OjN5d73IgHDiipK4R8wiy3TDyVEH/DaPnGypb2x/iC9Uf/pqO9OlqGdcESVvTpkPNZMgtx8tXxIrliN/cRfRK17Q/57luV+vAuU7qd5dvOoxdJ8gzsNzFm2i6tL84rSEgpWQ+wZ4MR705GffCo+rr8+4OeEiyRrx9S7y4ZhA8fbScc5l5mTQpVLHeKxlgn+FxopPCtpoiDpJ3a9hoi50F59Rtfn6y0k4KkPppVkGvDoTYgXiVjNbKUA3J5gu1XX2VZ4gE1Lyj0ZNW",
            "ddlRollDesc": "17",
            "ddlDistricts": "-1",
            "ddlACs": "-1",
            "ddlPARTs": "-1",
        }
        # print(form_dict)
        yield FormRequest.from_response(
            response,
            url="http://election.uk.gov.in/",
            method="POST",
            formdata=form_dict,
            # dont_click="True",
            callback=self.dist_parser,
        )

        # This function will select the mother_roll from drop down list and raise request to parse the names of districts

    def dist_parser(self, response):
        # "This fuction will parse the names of all the districts in the state and will raise another request to get all ac in the district."

        dist_names = response.xpath(
            '//select[@id="ddlDistricts"]/option/text()'
        ).extract()
        print(dist_names)
        dist_values = response.xpath(
            '//select[@id="ddlDistricts"]/option/@value'
        ).extract()
        print(dist_values)

        form_dict = {
            "__EVENTTARGET": "ddlDistricts",
            "__EVENTARGUMENT": response.css(
                "#__EVENTARGUMENT::attr(value)"
            ).extract_first(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract_first(),
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract(),
            "__VIEWSTATEGENERATOR": "CA0B0334",
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract(),
            "ddlRollDesc": "17",
            "ddlDistricts": "",
            "ddlACs": "-1",
            "ddlPARTs": "-1",
        }
        # print(form_dict)

        for dist in dist_values[1:]:
            form_dict["ddlDistricts"] = dist
            yield FormRequest.from_response(
                response,
                url="http://election.uk.gov.in/",
                method="POST",
                formdata=form_dict,
                # dont_click="True",
                meta={"district_code": dist},
                callback=self.ac_parser,
            )

    def ac_parser(self, response):
        # ac_names=response.css('#ddlACs').extract()
        ac_names_1 = response.xpath('//*[@id="ddlACs"]/option').extract()
        # ac_names_1=response.xpath('//*[@id="ddlACs"]/option/text()').extract()

        ac_values = response.xpath('//select[@id="ddlACs"]/option/@value').extract()
        # print(ac_names)
        print(ac_names_1)
        print(ac_values)

        form_dict = {
            "__EVENTTARGET": "ddlACs",
            "__EVENTARGUMENT": response.css("#__EVENTARGUMENT::attr(value)").extract(),
            "__LASTFOCUS": response.css("#__LASTFOCUS::attr(value)").extract(),
            "__VIEWSTATE": response.css("#__VIEWSTATE::attr(value)").extract(),
            "__VIEWSTATEGENERATOR": "CA0B0334",
            "__EVENTVALIDATION": response.css(
                "#__EVENTVALIDATION::attr(value)"
            ).extract(),
            "ddlRollDesc": "17",
            "ddlDistricts": response.meta["district_code"],
            "ddlACs": "",
            "ddlPARTs": "-1",
        }
        # print(form_dict)
        # print(type(ac_values))

        for ac in ac_values:

            form_dict["ddlACs"] = ac
            yield FormRequest.from_response(
                response,
                url="http://election.uk.gov.in/",
                method="POST",
                formdata=form_dict,
                # meta={"ac_names": ac_names[i]},
                callback=self.save_data,
            )

    def save_data(self, response):
        final_table = response.xpath('//select[@id="ddlPARTs"]/option/text()').extract()
        # print(final_table)
        table_list = pd.DataFrame(final_table, columns=["Polling_Station_Name"])
        # table_list = table_list.iloc[1:, 0:]
        print(table_list)


def main():

    process = CrawlerProcess()
    process.crawl(psdataUttarakhandscraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
