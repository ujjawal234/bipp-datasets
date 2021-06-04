# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy import Request


class psdataWBscraper(scrapy.Spider):
    name = "psdataWB"
    
    start_urls = [
        "https://www.elections.in/west-bengal/polling-booths/"
    ]


    
    def parse(self, response):
        print(response.text)
        #table_ps= response.css('body > div.container-fluid > div > div:nth-child(11) > div > div.col-md-9.mainleft > div > div.mob-table > table').extract()
        #print(table_ps)
        
        #yield Request(response.url, callback=self.save_data)
       
    def save_data(self, response):
        pass
        
    
    
        
def main():

      process = CrawlerProcess()
      process.crawl(psdataWBscraper)
      process.start()  # the script will pause here and wait for crawling to complete


if __name__ == "__main__":
    main()
    

        
    
        

