import json
from pathlib import Path
import hashlib
import pandas as pd

import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess

import urllib.parse as urlparse
from urllib.parse import parse_qs



class DealerSpider(scrapy.Spider):
    name = 'dealer'
    start_urls = [
        'https://reports.dbtfert.nic.in/mfmsReports/StateWiseDealerList',
    ]
    project_dir = str(Path(__file__).resolve().parents[0])
    parent_folder = project_dir + "/data/raw/"
    dataset = []
    failed_requests = []
      

    def parse(self, response):
        with open('temp.html', 'w') as f:
            f.write(response.css('html').get())
        list1 = []  #list1 stores all urls
        list2 = []  #list2 stores dictionary which has stateid as key and statename as value
        for x in response.css('td:nth-child(2) a').extract():
            temp = x.split('>')
            dictionary = {
                "id": temp[0].split('stateId')[1].replace('"',''),
                "name": temp[1].replace('</a','') 
            }
            list2.append(dictionary)
        
        for x in response.css('td~ td+ td a::attr(href)').extract():
            list1.append(x)
        #for x in list1:
        for i in range(0,6):
            x = list1[i]   #to debug the code, i is 0 to 5(both inclusive)
            next_page = x
            temp = x.split('stateId=')
            ids = temp[1].split('&') # '1' to '36'
            dealer_nature = temp[1].split('=')[1]
            dealer_type = temp[0].split('dealer_type=')[1].split('&amp;is')[0]
            #'1' '2'
            if(dealer_type=='1'):
                dealer_type = "Retailers"
            else:
                dealer_type = "Wholesalers"
            state_name = ""
            for y in list2:
                if(y["id"]==ids):
                      state_name = y["name"]
            # 3:private wholesaler
            #5: private retailer
            #4: pacs(both)
            # 2: dist coop society(both)
            # 1: state marketing federations (both)
            #10: inst. buyers
            #6:depot
            
            if(dealer_nature=='1'):
                dealer_nature = "State Marketing Federations"
            elif(dealer_nature=='2'):
                dealer_nature = "Dist Coop society"
            elif(dealer_nature=='3'):
                dealer_nature = "State Marketing Federations"
            elif(dealer_nature=='4'):
                dealer_nature = "Pacs"
            elif(dealer_nature=='5'):
                dealer_nature = "Private Retailer"
            elif(dealer_nature=='10'):
                dealer_nature = "Inst. Buyers"
            meta = {
                'ids': ids,
                'dealer_nature': dealer_nature,
                'dealer_type': dealer_type,
                'state_name': state_name,
            }  #meta stores the stateid, dealer nature and dealer type taken from url
            print(list1)
            print(meta, next_page)
            if next_page is not None:
                next_page = response.urljoin(next_page)
                yield scrapy.Request(next_page, callback=self.collect_data, meta= meta)
        
    def collect_data(self, response):
        store_table = response.css('#right > center > table:nth-child(2)').get()
        list_df = pd.read_html(store_table)
        print(list_df)
        index = 0
        data = list_df[index]  #couldn't run so don't know what should be in third bracket
        #print(data)
        
        
        file_path = self.parent_folder + '/' + str(response.meta['state_name']) + "/" + str(response.meta['dealer_type'])
        file_name = response.meta['dealer_nature']+'.csv'
        self.ensure_directory(file_path)
        data.to_csv(file_path+'/'+file_name)
        
        #self.dataset.append()
        
    def ensure_directory(self, file_path):
            """
            This function ensures the directory path provided in file_path
            exists, if not creates the path
            """

            path_parts = file_path.split('/')
            for i in range(1, len(path_parts)+1):
                present_path = '/'.join(path_parts[:i])
                Path(present_path).mkdir(exist_ok=True)

def main():
    """
    This is the script to scrape raw data from PMGSY website for Physical Monitoring of Works dataset.

    USAGE: python get_pmgsy_data.py
    """
    # Create a Crawler Process with custom settings
    process = CrawlerProcess(settings={'RETRY_ENABLED':True})
    process.crawl(DealerSpider)
    process.start()  # the script will block here until the crawling is finished


if __name__ == "__main__":
    main()