import requests
import json
import pandas as pd
import time
import pickle
import concurrent.futures
from pandas.core.common import flatten


class StockX():
    '''Scraper for the website www.stockx.com'''

    def __init__(self):
        '''Initiates a StockX scraper object'''
        self.api = 'https://stockx.com/api/browse?'
        self.sales_api = 'https://stockx.com/api/products/'
        self.headers = None
        self._scraped_products = []
        self._scraped_sales_history = []

    @property
    def api(self):
        '''str: url of StockX products API.'''
        return self.api

    @property
    def sales_api(self):
        '''str: url of StockX products API.'''
        return self.sales_api

    @property
    def headers(self):
        '''dict: headers used in the get request.'''
        return self.headers

    @property
    def _scraped_products(self):
        '''list of json objects: stores json information of scraped products'''
        return self._scraped_products

    @headers.setter
    def set_headers(self, headers):
        '''list of json objects: stores json information of scraped products' sales history'''
        self.headers = headers

    def reset_scrape(self):
        '''Resets the scraped products and products' sale history information'''
        self._scraped_products = []
        self._scraped_sales_history = []

    def _scrape(self, url):
        '''Scrapes the product information for a particular product.

        Args:
            url: url of the product to be scraped.

        Returns:
            None

        '''
        r = requests.get(url, headers=self.headers)
        data = json.loads(r.text)
        self._scraped_products.append(data)
        return

    def _scrape_sales_history(self, url):
        '''Scrapes the sales history information for a particular product.

        Args:
            url: url of the product sales to be scraped.

        Returns:
            None

        '''
        r = requests.get(url, headers=self.headers)
        data = json.loads(r.text)
        data['ProductActivity'][0]['productId'] = url.split('/')[5]
        self._scraped_sales_history.append(data)
        return

    def product_info_to_dataframe(self):
        '''Converts scraped product information into a pandas DataFrame

        Returns:
            DataFrame

        '''
        dfs = [pd.json_normalize(data['Products'])
               for data in self._scraped_products]
        return pd.concat(dfs, ignore_index=True)

    def sales_history_to_dataframe(self):
        '''Converts scraped product information into a pandas DataFrame

        Returns:
            DataFrame

        '''
        dfs = [pd.json_normalize(data['ProductActivity'])
               for data in self._scraped_sales_history]
        return pd.concat(dfs, ignore_index=True)

    def get_category_urls(self, tag, category):
        '''Obtains urls for all products based on a particular tag and category.

        Args:
            tag (str): tag describing product.
            category (str): category describing product.
        Returns:
            list of str: urls for all products.
        Examples: 
            >>> get_category_urls('air jordan', 'sneakers')


        '''

        r = requests.get(
            f'{self.api}_tags={tag}&productCategory={category}&page=1', headers=self.headers)
        data = json.loads(r.text)
        last_page = int(data['Pagination']['lastPage'].split(
            'page=')[1].split('&')[0])

        urls = [
            f'{self.api}_tags={tag}&productCategory={category}&page={i}' for i in range(1, last_page + 1)
        ]
        return urls

    def get_product_info(self, urls):
        '''Scrapes product information.

        Args:
            urls (list of str): urls to scrape.
        Returns:
            None

        '''
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self._scrape, urls)

    def get_product_sales_history(self, products):
        '''Scrapes product sales history information.

        Args:
            urls (list of str): urls to scrape.
        Returns:
            None

        '''
        urls = [
            f'https://stockx.com/api/products/{product}/activity?state=480&currency=USD&limit=10000&page=1' for product in products]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self._scrape_sales_history, urls)


def main():
    scraper = StockX()  # create a scraper
    # get all the urls related to products from a certain category
    category_urls = scraper.get_category_urls('air jordan', 'sneakers')
    scraper.get_product_info(category_urls)  # scrape product info
    # convert product info to a DataFrame
    product_info = scraper.product_info_to_dataframe()
    products = list(product_info['id'])
    # scrape sales history for each product
    scraper.get_product_sales_history(products)
    # convert product info to a DataFrame
    df = scraper.sales_history_to_dataframe()
    # fill all rows with the productId
    df['productId'] = df['productId'].fillna(method='ffill')
    # merge product info with product sales history
    df = df.merge(product_info, how='left', left_on='productId', right_on='id')
    # save DataFrame to csv
    df.to_csv('full_data_latest_with_all_info.csv', index=False)


if __name__ == '__main__':
    main()
