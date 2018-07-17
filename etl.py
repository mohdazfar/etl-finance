from googlefinance.client import get_price_data
from dateutil.rrule import rrule, MONTHLY
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import logging
import requests
import pymysql
import time
import json

from credentials import Credentials

logging.basicConfig(format='%(asctime)s %(message)s')

class ETLPipeline(object):

    def __init__(self, table_name):
        self.table_name = table_name
        self.MYSQL_USER = Credentials.MYSQL_USER
        self.MYSQL_PASSWORD = Credentials.MYSQL_PASSWORD
        self.MYSQL_DB_NAME = Credentials.MYSQL_DB_NAME
        self.NYTIMES_API_KEY = Credentials.NTYIMTES_API_KEY

    def to_date(self, date_column):
        '''
        Convert Pandas string to Datetype column
        :param date_column: Pandas date string column
        :return:
        '''
        return date_column.apply(lambda x: pd.to_datetime(x).strftime('%d-%m-%Y'))

    def to_timestamp(self, date_column):
        '''
        Try different date patterns and returns the timestamp of the date
        :param date_column: Pandas date string column
        :return:
        '''
        try:
            # For NYTIMES date format
            date_column = date_column.apply(lambda x: pd.to_datetime(x).strptime(x, "%Y-%m-%dT%H:%M:%S+%f"))
            return date_column.values.astype(np.int64) // 10**9
        except TypeError :
            try:
                # For Google finance date format
                return date_column.values.astype(np.int64) // 10**9
            except:
                date_column = date_column.apply(lambda x: pd.to_datetime(x).strptime(x, "%Y-%m-%d %H:%M:%S"))
                return date_column.values.astype(np.int64) // 10 ** 9

    def setup_table(self, sql_cursor):
        pass

    def insert_to_db(self, sql_cursor):
        pass

    def extract(self):
        '''
        Get the data from google finance API (stock prices),
        NYTimes API (news data), restapi.io (FOREX data)
        :return: appended dataframe containing data from all given stocks
        '''
        return

    def clean(self):
        '''
        Basic cleaning applied by converting data extracted data into
        dataframes. Reason for using dataframes is to visualize the
        data at any point during cleaning. Also 'pandas' allows us
        to clean dataframes very easily
        :return: cleaned dataframe
        '''
        return

    def transform(self):
        '''
        Basic transformation is done by adding few important columns.
        Date columns are also transformed for consistency at later point
        to query the results easily
        :return:
        '''
        return

    def load(self):
        logging.warning('Connecting to MySQL. Loading table {}'.format(self.table_name))
        conn = pymysql.connect(host='localhost',
                               port=3306,
                               user= self.MYSQL_USER,
                               passwd= self.MYSQL_PASSWORD,
                               db=self.MYSQL_DB_NAME)
        cur = conn.cursor()
        cur.execute('''SHOW TABLES LIKE "{}"'''.format(self.table_name))
        check = cur.fetchone()
        if check is not None:
            self.insert_to_db(cur)
        else:
            self.setup_table(cur)
            self.insert_to_db(cur)

        conn.commit()
        cur.close()
        logging.warning('Successfully completed loading the data to the {} table'.format(self.table_name))

    def run(self):
        self.extract()
        self.clean()
        self.transform()
        self.load()


class StockETL(ETLPipeline):

    def __init__(self, stocks, interval, stock_market, period):
        ETLPipeline.__init__(self, table_name='stock_ticks')
        self.stocks = stocks
        self.interval = interval
        self.stock_market = stock_market
        self.period = period


    def extract(self):
        df = pd.DataFrame()
        for stock in self.stocks:
            param = {
                'q': stock,
                'i': self.interval,
                'x': self.stock_market,
                'p': self.period
            }
            sub_df = get_price_data(param)
            sub_df['StockName'] = stock
            df = df.append(sub_df)
            logging.warning('Getting data from Google Finance API. Current stock {}'.format(stock))
        self.df = df


    def clean(self):
        logging.warning('Starting to clean Stock data...')
        # Check for NANs and replace them by column average
        check = self.df.isnull().values.any()
        if check:
            mean = self.df.mean # mean of each column of the dataframe
            replace_values = {'Open': mean[0], 'High': mean[1], 'Low': mean[2],
                              'Close': mean[3], 'Volume':mean[4] }
            self.df = self.df.fillna(value=replace_values)
        # Converting stock values to float and Volumes to int
        # Usually not required
        self.df[['Open', 'High', 'Low',  'Close']] = \
            self.df[['Open', 'High', 'Low',  'Close']].astype(float)
        self.df['Volume'] = self.df['Volume'].astype(int)



    def transform(self):
        logging.warning('Performing data transformation')
        # Resetting the index to column because we have to use
        # this data to load to database
        self.df.index.name = 'Date'
        self.df = self.df.reset_index()
        self.df['Short_date'] = self.to_date(self.df['Date'])
        self.df['Timestamp'] = self.to_timestamp(self.df['Date'])
        del self.df['Date'] # Delete Date column b/c it is not required now

        # Making 2 new columns
        # Percentage change in price and volume
        self.df['pct_change_returns'] = (self.df['Open'] /
                            self.df['Close'].shift(1) - 1).fillna(0)

        self.df['pct_change_volume'] =  (self.df['Volume'] /
                            self.df['Volume'].shift(1) - 1).fillna(0)



    def setup_table(self, sql_cursor):
        logging.warning('Setting up TABLES and creating INDEX')
        sql_cursor.execute('''CREATE TABLE IF NOT EXISTS stock_ticks(
		time_stamp BIGINT, stock_name VARCHAR(6), price_open FLOAT, price_high FLOAT,
        price_low FLOAT, price_close FLOAT, volume BIGINT, pct_ret FLOAT, pct_vol FLOAT);''')

        sql_cursor.execute('''CREATE UNIQUE INDEX idx_stocks ON stock_ticks (time_stamp, stock_name) ''')

    def insert_to_db(self, sql_cursor):
        for i, row in self.df.iterrows():
            sql_cursor.execute("INSERT INTO stock_ticks "
                "(time_stamp, stock_name, price_open, price_high, "
                "price_low, price_close, volume, pct_ret, pct_vol)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE "
                "time_stamp=time_stamp, stock_name=stock_name, price_open=price_open, price_high=price_high, "
                "price_low=price_low, price_close=price_close, volume=volume, pct_ret=pct_ret, pct_vol=pct_vol",
            (row['Timestamp'], row['StockName'],  row['Open'], row['High'], row['Low'],
             row['Close'], row['Volume'], row['pct_change_returns'], row['pct_change_volume']))



class NewsETL(ETLPipeline):

    '''
    NEWS_DESK_VALUES = ['Adventure Sports', 'Arts & Leisure', 'Arts', 'Automobiles', 'Blogs', 'Books',
            'Booming', 'Business Day', 'Business', 'Cars', 'Circuits', 'Classifieds', 'Connecticut', 'Crosswords & Games',
             'Culture', 'DealBook', 'Dining', 'Editorial', 'Education', 'Energy', 'Entrepreneurs', 'Environment', 'Escapes',
             'Fashion & Style', 'Fashion', 'Favorites', 'Financial', 'Flight', 'Food', 'Foreign', 'Generations', 'Giving',
             'Global Home', 'Health & Fitness', 'Health', 'Home & Garden', 'Home', 'Jobs', 'Key', 'Letters', 'Long Island',
             'Magazine', 'Market Place', 'Media', "Men's Health", 'Metro', 'Metropolitan', 'Movies', 'Museums', 'National',
            'Nesting', 'Obits', 'Obituaries', 'Obituary', 'OpEd', 'Opinion', 'Outlook', 'Personal Investing', 'Personal Tech',
            'Play', 'Politics', 'Regionals', 'Retail', 'Retirement', 'Science', 'Small Business', 'Society', 'Sports', 'Style',
            'Sunday Business', 'Sunday Review', 'Sunday Styles', 'T Magazine', 'T Style', 'Technology', 'Teens', 'Television',
            'The Arts', 'The Business of Green', 'The City Desk', 'The City', 'The Marathon', 'The Millennium',
            'The Natural World', 'The Upshot', 'The Weekend', 'The Year in Pictures', 'Theater', 'Then & Now', 'Thursday Styles',
             'Times Topics', 'Travel', 'U.S.', 'Universal', 'Upshot', 'UrbanEye', 'Vacation', 'Washington', 'Wealth', 'Weather',
            'Week in Review', 'Week', 'Weekend', 'Westchester', 'Wireless Living', "Women's Health", 'Working',
            'Workplace', 'World', 'Your Money']

    SECTION_FIELDS = ['Arts', 'Automobiles', 'Autos', 'Blogs', 'Books', 'Booming', 'Business', 'Business Day',
            'Corrections', 'Crosswords & Games', 'Crosswords/Games', 'Dining & Wine', 'Dining and Wine',
            "Editors' Notes", 'Education', 'Fashion & Style', 'Food', 'Front Page', 'Giving', 'Global Home',
             'Great Homes & Destinations', 'Great Homes and Destinations', 'Health', 'Home & Garden',
            'Home and Garden', 'International Home', 'Job Market', 'Learning', 'Magazine', 'Movies',
            'Multimedia', 'Multimedia/Photos', 'N.Y. / Region', 'N.Y./Region', 'NYRegion', 'NYT Now',
            'National', 'New York', 'New York and Region', 'Obituaries', 'Olympics', 'Open', 'Opinion',
            'Paid Death Notices', 'Public Editor', 'Real Estate', 'Science', 'Sports', 'Style', 'Sunday Magazine',
             'Sunday Review', 'T Magazine', 'T:Style', 'Technology', 'The Public Editor', 'The Upshot', 'Theater',
            'Times Topics', 'TimesMachine', "Today's Headlines", 'Topics', 'Travel', 'U.S.', 'Universal',
             'UrbanEye', 'Washington', 'Week in Review', 'World', 'Your Money']
    '''

    def __init__(self, api_key, start_year, start_month, end_year, end_month):
        ETLPipeline.__init__(self, table_name='news')
        self.api_key = api_key
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month

        if(start_year > end_year):
            raise ValueError('Start Year cannot be greater than End Year')
        if(12 <= start_month <= 0 and 12 <= start_month <= 0):
            raise ValueError('Month value should be between 1 and 12. 1=January and 12=December')



    def getMonthsBetween(self):
        start_date_string = '{0}/{1}'.format(self.start_month, self.start_year)
        end_date_string = '{0}/{1}'.format(self.end_month, self.end_year)

        start_date = datetime.strptime(start_date_string, '%m/%Y')
        end_date = datetime.strptime(end_date_string, '%m/%Y')

        months_diff = [[dt.strftime("%m"), dt.strftime("%Y")] for dt in
                  rrule(MONTHLY, dtstart=start_date, until=end_date)]

        return months_diff



    def extract(self):
        logging.warning('Extracting news data from NYTimes API')

        IMPORTANT_FIELDS = ['Business', 'Foreign', 'Business Day', 'Financial',
                        'National', 'Small Business', 'Technology', 'World']
        URI_ROOT = 'https://api.nytimes.com/svc/archive/v1'
        months = self.getMonthsBetween()
        data = []
        df = pd.DataFrame()

        for month in months:
            URL = '{0}/{1}/{2}.json?api-key={3}'.format(URI_ROOT,
                                                        int(month[1]),
                                                        int(month[0]),
                                                        self.api_key)
            response = requests.get(URL)
            response_json = response.json()

            for news in response_json['response']['docs']:
                try:
                    # Selected few fields randomly which can impact the finance industry
                    # news['news_desk'] is a field which returns the news category
                    if( news['new_desk'] in IMPORTANT_FIELDS):
                        keywords = [i['value'] for i in  news['keywords']]

                        data.append((news['pub_date'],
                                     news['snippet'],
                                     news['headline']['main'],
                                     keywords))
                except KeyError:
                    if (news['news_desk'] in IMPORTANT_FIELDS):
                        keywords = [i['value'] for i in news['keywords']]

                        data.append((news['pub_date'],
                                     news['snippet'],
                                     news['headline']['main'],
                                     keywords))
            sub_df = pd.DataFrame(data, columns=['pub_date', 'snippet', 'headline', 'keywords'])
            df = df.append(sub_df)
            break
        self.df = df


    def clean(self):
        logging.warning('Starting to clean news data...')
        # Convert string columns to lowercase
        self.df['snippet'] = self.df['snippet'].str.lower()
        self.df['headline'] = self.df['headline'].str.lower()
        self.df['keywords'] = self.df['keywords'].apply(
                                lambda x: [str(i).lower() for i in x])

        # Filter empty snippet and headline rows
        self.df = self.df[self.df['snippet'] != '']
        self.df = self.df[self.df['headline'] != '']


    def transform(self):
        logging.warning('Performing data transformation on news data')

        # Creating date columns with timestamp and short date
        self.df['short_date'] = self.to_date(self.df['pub_date'])
        self.df['timestamp'] = self.to_timestamp(self.df['pub_date'])

        # Delete Date column b/c it is not required now
        del self.df['pub_date']


    def setup_table(self, sql_cursor):
        logging.warning('Setting up TABLES and creating INDEX')
        sql_cursor.execute('''CREATE TABLE IF NOT EXISTS news(
        time_stamp BIGINT, short_date DATE, snippet TEXT, headline TEXT(200), keywords JSON );''')

        sql_cursor.execute(
            '''CREATE UNIQUE INDEX idx_news ON news (time_stamp, headline) ''')


    def insert_to_db(self, cursor):
        for i, row in self.df.iterrows():
            json_data = json.dumps(row['keywords'])
            date = datetime.strptime(row['short_date'], '%d-%m-%Y')
            cursor.execute("INSERT INTO news "
                "(time_stamp, short_date, snippet, headline, keywords)"
               " VALUES (%s,%s, %s, %s, %s) ON DUPLICATE KEY UPDATE "
                "time_stamp=time_stamp, short_date=short_date, "
                "snippet=snippet, headline=headline, keywords=keywords ",
            (row['timestamp'], date, row['snippet'],
             row['headline'], json_data))



class ForexETL(ETLPipeline):
    
    def __init__(self, start_date, end_date):
        ETLPipeline.__init__(self, table_name='forex')
        self.ROOT_URI_FOREX = 'https://ratesapi.io/api/'
        self.ROOT_URI_BTC = 'https://api.coindesk.com/v1/bpi/historical/close.json'
        self.start_date = start_date
        self.end_date = end_date
        self.delta = self.end_date - self.start_date  # timedelta

        if self.start_date > self.end_date:
            raise ValueError('Start date cannot be greater than End date')


    def get_data(self, url):
        response_forex = requests.get(url).json()

        # Filter specific currencies we are interested in
        return [response_forex['rates']['EUR'],
                         response_forex['rates']['GBP'],
                         response_forex['rates']['SEK'],
                         response_forex['rates']['DKK']]
    
    def extract(self):
        self.forex_data = {}
        
        # extract forex data
        for i in range(self.delta.days + 1):
            current_date = str(self.start_date + timedelta(i))
            url_forex = '{0}{1}?base=USD'.format(self.ROOT_URI_FOREX,
                                                 current_date)
            try:
                required_data = self.get_data(url_forex)
                self.forex_data[current_date] = required_data
            except requests.HTTPError:
                logging.warning(
                    "restapi.io didn't respond. Trying again ...")
                time.sleep(5)
                required_data = self.get_data(url_forex)
                self.forex_data[current_date] = required_data
        
        # extract btc data
        url_btc = '{0}?start={1}&end={2}&currency=USD'.format(self.ROOT_URI_BTC,
                                                              str(self.start_date),
                                                              str(self.end_date))
        btc_data = requests.get(url_btc).json()
        self.btc_data = btc_data['bpi']



    def clean(self):
        logging.warning('Starting to clean FOREX and BTC Data...')
        self.df = pd.DataFrame([self.forex_data, self.btc_data]).T
        self.df[['usd_to_eur','usd_to_gbp','usd_to_sek','usd_to_dkk']] = \
            pd.DataFrame(self.df[0].values.tolist(), index=self.df.index)
        del self.df[0]
        self.df.columns = ['usd_to_btc', 'usd_to_eur','usd_to_gbp',
                           'usd_to_sek','usd_to_dkk']

        # Check for NANs and replace them by column average
        check = self.df.isnull().values.any()
        if check:
            mean = self.df.mean  # mean of each column of the dataframe
            replace_values = {'usd_to_btc': mean[0], 'usd_to_eur': mean[1], 'usd_to_gbp': mean[2],
                              'usd_to_sek': mean[3], 'usd_to_dkk': mean[4]}
            self.df = self.df.fillna(value=replace_values)

        self.df= self.df.astype(float)


    def transform(self):
        logging.warning('Performing data transformation on FOREX and BTC data')
        # Making columns for pct change
        self.df[['usd_to_btc_delta',
                 'usd_to_eur_delta',
                 'usd_to_gbp_delta',
                 'usd_to_sek_delta',
                 'usd_to_dkk_delta']] = (self.df / self.df.shift(1) - 1).fillna(0)


        self.df.index.name = 'date'
        self.df = self.df.reset_index()


    def setup_table(self, sql_cursor):
        logging.warning('Setting up TABLES and creating INDEX for forex table')
        sql_cursor.execute('''CREATE TABLE IF NOT EXISTS forex(
        short_date DATE, usd_to_btc FLOAT, usd_to_eur FLOAT, usd_to_gbp FLOAT, usd_to_sek FLOAT, usd_to_dkk FLOAT, 
        usd_to_btc_delta FLOAT, usd_to_eur_delta FLOAT, usd_to_gbp_delta FLOAT, usd_to_sek_delta FLOAT, usd_to_dkk_delta FLOAT);''')

        sql_cursor.execute('''CREATE UNIQUE INDEX idx_forex ON forex (short_date) ''')


    def insert_to_db(self, sql_cursor):
        for i, row in self.df.iterrows():
            date = datetime.strptime(row['date'], '%Y-%m-%d')
            sql_cursor.execute("INSERT INTO forex "
                "(short_date, usd_to_btc, usd_to_eur,usd_to_gbp, usd_to_sek, usd_to_dkk,"
                "usd_to_btc_delta, usd_to_eur_delta, usd_to_gbp_delta, usd_to_sek_delta, usd_to_dkk_delta)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE "
                "short_date=short_date, usd_to_btc=usd_to_btc, usd_to_eur=usd_to_eur, usd_to_gbp=usd_to_gbp, usd_to_sek=usd_to_sek, usd_to_dkk=usd_to_dkk,"
                "usd_to_btc_delta=usd_to_btc_delta, usd_to_eur_delta=usd_to_eur_delta, usd_to_gbp_delta=usd_to_gbp_delta, "
                           "usd_to_sek_delta=usd_to_sek_delta, usd_to_dkk_delta=usd_to_dkk_delta",
            (date, row['usd_to_btc'], row['usd_to_eur'], row['usd_to_gbp'], row['usd_to_sek'], row['usd_to_dkk'],
             row['usd_to_btc_delta'], row['usd_to_eur_delta'], row['usd_to_gbp_delta'], row['usd_to_sek_delta'], row['usd_to_dkk_delta']))




if __name__ == '__main__':
    # Stock data parameters
    stocks = ['MSFT', 'INTL', 'FB', 'IBM', 'GOOG', 'AAPL'] # Stock symbols
    interval = '86400' # Time interval in seconds. 86400 s = 1 Day
    stock_market = 'NASDAQ' # Stock exchange symbol on which stock is traded
    period = '2Y'  # Period (Ex: "1Y" = 1 year)

    # Stock Data ETL
    stock_pipeline = StockETL(stocks, interval, stock_market, period)
    stock_pipeline.run()

    # NYTIMES data parameters
    start = {'year': 2017, 'month': 1}
    until = {'year': 2018, 'month': 1}
    
    # NYTIMES ETL 
    news_pipeline = NewsETL(api_key=Credentials.NTYIMTES_API_KEY,
                            start_year=start['year'],
                            start_month=start['month'],
                            end_year=until['year'],
                            end_month=until['month'])

    news_pipeline.run()

    # FOREX Parameters
    start_date = date(2018, 1, 1)
    end_date = date(2018, 1, 15)
    
    
    # FOREX ETL
    forex_pipeline = ForexETL(start_date, end_date)
    forex_pipeline.run()
    
