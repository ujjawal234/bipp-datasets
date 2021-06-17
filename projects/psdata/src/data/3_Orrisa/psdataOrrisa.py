import scrapy
from scrapy import FormRequest, Request
from scrapy.crawler import CrawlerProcess


class psdataOrrisascraper(scrapy.Spider):
    name = "psdataOrrisa"
    custom_settings = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36"
    }

    # project_dir = str(Path(__file__).resolve().parents[2])
    # parent_folder = project_dir + "/data/raw/"

    def start_requests(self):
        # request to initiate the scraping
        yield Request("http://election.ori.nic.in/odishaceo/ViewEroll.aspx")

    def parse(self, response):
        # "This fuction will parse the names of all the districts in the state and will raise another request to get all ACS in the district."

        dist_names = response.xpath(
            '//select[@id="ddlDistrict"]/option/text()'
        ).extract()
        print(dist_names)

        dist_values = response.xpath(
            '//select[@id="ddlDistrict"]/option/@value'
        ).extract()
        i = 1
        for dist in dist_values[1:]:
            yield FormRequest.from_response(
                response,
                url="http://election.ori.nic.in/odishaceo/ViewEroll.aspx",
                method="POST",
                formdata={"ddlDistrict": dist},
                dont_click="True",
                callback=self.ac_parser,
            )
            i += 1

    def ac_parser(self, response):

        ac_list = response.xpath('//select[@id="ddlAC"]/option/text()').extract()
        ac_values = response.xpath('//select[@id="ddlAC"]/option/@value').extract()
        print(ac_list)
        print("*****")
        i = 1
        for ac in ac_values[1:]:
            yield FormRequest.from_response(
                response,
                url="http://election.ori.nic.in/odishaceo/ViewEroll.aspx",
                method="POST",
                formdata={"ddlDistrict": ac, "ddlAC": ac},
                meta={"ac_names": ac_list[i]},
                dont_click="True",
                callback=self.save_data,
            )
            i += 1

    def save_data(self, response):
        final_table = response.xpath('//select[@id="ddlPart"]/option/text()').extract()
        print(final_table)


def main():

    process = CrawlerProcess()
    process.crawl(psdataOrrisascraper)
    process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
