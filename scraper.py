import pymongo
import requests
import logging
import json
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool


logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level=logging.DEBUG)


class HotlineScraper():

    def __init__(self):
        self.base_url = 'https://hotline.ua'
        self.url = 'https://hotline.ua/mobile/mobilnye-telefony-i-smartfony/294245/'
        self.header = {'user-agent': 'Mozilla/5.0'}
        self.pool = ThreadPool(5)
        self.collection = self.db_connection()
        self.session = requests.session()

    def db_connection(self):
        connection = pymongo.MongoClient('localhost', 27017)
        db = connection['Hotline']
        collection = db['iPhone']
        return collection

    def get_listing_page(self):
        page = self.session.get(self.url, headers=self.header)
        search_listig_count = BeautifulSoup(page.text, 'html.parser')
        listing_count = search_listig_count.find('div', class_='pages-list cell-sm').find_all('a')[-1].text
        return int(listing_count)

    def get_item_link(self, page_url):
        page = self.session.get(page_url, headers=self.header)
        soup = BeautifulSoup(page.text, 'html.parser')
        container = soup.find('div', class_='tile-viewbox').find('ul', class_='products-list cell-list').find_all('li')
        for item in container:
            try:
                link = item.find('div', class_='item-info').find('p').find('a')['href']
                name = item.find('div', class_='item-info').find('p').find('a').text.replace(' ', '').replace('\n', '')
                product_id = str(item.find('div', class_='item-compare').find('a').span).split('=')[1].replace('"','').replace('></span>','')
            except:
                pass
            search_data = {
                'product_id': product_id,
                'name': name,
                'link': link,
            }
            check_query = {'product_id': product_id}
            if self.collection.find(check_query):
                self.collection.update_one(check_query,
                {'$set': search_data})
                logging.info(f'Item by {product_id} was update in collection')
            else:
                self.collection.insert_one(search_data)
                logging.info(f'Item by {product_id} was write to collection')

    def get_response_page(self):
        pages_url = []
        for num_page in range(self.get_listing_page()):
            pages_url.append(self.url+'?p='+str(num_page))
        self.pool.map(self.get_item_link, pages_url)

    def parse_product_data(self, product_url):
        page = self.session.get(product_url.split('load-prices/')[0], headers=self.header)
        soup = BeautifulSoup(page.text, 'html.parser')
        csrf_token = soup.select_one('meta[name="csrf-token"]')['content']
        page = self.session.get(product_url, headers={
            'user-agent': 'Mozilla/5.0',
            'content-type': 'application/x-www-form-urlencoded',
            'x-csrf-token': csrf_token,
            })
        json_page = page.json()
        proposals = json_page['filters']['counts']['cond_new']
        shop_info = []
        for shop in json_page['prices']:
            data = {
                'shop_name': shop['firm_title'],
                'shop_website': shop['firm_website'],
                'name_of_product': shop['complaint_title'],
                'date_of_publication': shop['date'],

            }

    def scrap_product_data(self):
        result = self.collection.find({}, {'product_id':1, 'link':1, '_id': 0})
        url_list = []
        for data in result:
            url_list.append(self.base_url + data['link'] + 'load-prices/')
        self.pool.map(self.parse_product_data, url_list)

    def main(self):
        self.get_response_page()
        #self.scrap_product_data()


if __name__ == "__main__":
    scraper = HotlineScraper()
    scraper.main()

