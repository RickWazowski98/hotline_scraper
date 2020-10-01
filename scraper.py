import pymongo
import requests
import logging
import json
import datetime
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

    def check_and_delete_duplicate_in_collection(self):
        dublicate_list = self.collection.aggregate([
            {"$group":{"_id":"$product_id","product_id":{"$first":"$product_id"},"count":{"$sum":1}}},
            {"$match":{"count":{"$gt":1}}},
            {"$project":{"product_id":1,"_id":0}},
            {"$group":{"_id":None,"duplicate_names":{"$push":"$product_id"}}},
            {"$project":{"_id":0,"duplicate_names":1}}
        ])
        for item in dublicate_list:
            duplicate_count = len(item['duplicate_names'])
            logging.info(f"Find {duplicate_count} duplicate.")
            for dupl in item['duplicate_names']:
                self.collection.delete_one({'product_id': dupl})
                logging.info(f'Delete duplicate by product_id: {dupl}.')

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
            query = {'product_id': product_id}
            self.collection.update_one(
                query,
                {'$set': search_data},
                upsert=True,
                )
            logging.info(f'Item by {product_id} was update in collection.')

    def get_response_page(self):
        pages_url = []
        for num_page in range(self.get_listing_page()):
            pages_url.append(self.url+'?p='+str(num_page))
        try:
            self.pool.map(self.get_item_link, pages_url)
        except:
            pass

    def parse_product_data(self, product_url):
        logging.info(f'Data parsing has been started.')
        page = self.session.get(product_url.split('load-prices/')[0], headers=self.header)
        soup = BeautifulSoup(page.text, 'html.parser')
        csrf_token = soup.select_one('meta[name="csrf-token"]')['content']
        page = self.session.get(product_url, headers={
            'user-agent': 'Mozilla/5.0',
            'content-type': 'application/x-www-form-urlencoded',
            'x-csrf-token': csrf_token,
            })
        logging.info(f'The token has been received.')
        json_page = page.json()
        proposals = json_page['filters']['counts']['cond_new']
        product_id = json_page['prices'][0]['cardid']
        logging.info(f'Data collection for product: {product_id} has been started.')
        link_to_product = product_url.split('load-prices/')[0]
        shop_info = []
        for shop in json_page['prices']:
            data = {
                'shop_name': shop['firm_title'],
                'shop_website': shop['firm_website'],
                'shop_rating': str(int(shop['shopRating']['rating'])/10) if shop['shopRating']['rating'] else '',
                'quantity': shop['shopRating']['reviews_quantity'],
                'name_of_product': shop['complaint_title'],
                'date_of_publication': shop['date'],
                'currency': shop['currency'],
                'delivery': {
                    'free_delivery': shop['delivery']['has_free_delivery'],
                    'same_region_city':shop['delivery']['same_region_city'],
                    'same_city_delivery': shop['delivery']['same_city'],
                    'same_region_delivery': shop['delivery']['same_region'],
                    'another_city': shop['delivery']['another_city'],
                },
                'guarancy': shop['guarantee_format'],
                'sales': shop['has_sales'],
                'price_uah': str(shop['price_uah_real_raw']) + ' UAH',
                'price_usd': str(shop['price_usd_real']).replace('&nbsp;','') + ' USD',

            }
            shop_info.append(data)
        
        self.collection.update_one(
            {'product_id': product_id},
                {'$set':{
                    'proposals': proposals,
                    'product_url': link_to_product,
                    'shops_info': shop_info,
                    }
                }
            )
        logging.info(f'Data collection for product: {product_id} has been completed.')

    def scrap_product_data(self):
        result = self.collection.find({}, {'product_id':1, 'link':1, '_id': 0})
        url_list = []
        for data in result:
            url_list.append(self.base_url + data['link'] + 'load-prices/')
        try:
            self.pool.map(self.parse_product_data, url_list)
        except:
            pass

    def main(self):
        start_time = datetime.datetime.now().replace(microsecond=0)
        # self.check_and_delete_duplicate_in_collection()
        self.get_response_page()
        self.scrap_product_data()
        end_time = datetime.datetime.now().replace(microsecond=0)
        run_time = end_time - start_time
        logging.info(f'Program execution time: {run_time}')



if __name__ == "__main__":
    scraper = HotlineScraper()
    scraper.main()

