import time
import pandas as pd
import tweepy
import requests
from datetime import datetime
from tweepy.errors import TweepyException, NotFound
from db_handler import insert_to_db, retrieve_data, QUERIES
import yfinance as yf


class TweetRetriever:
    """
    # https://developer.twitter.com/en/docs/twitter-api/rate-limits
    Retrieves tweets and their comments from the list of user accounts; saves tweets to the database.
        NOTE: comments could be extracted only up last 7 days, unless you have premium api. However, for tweets you can
        extract up to 3200 most recent tweets from user's timeline. For more check twitter's api.
    Built fot the purposes to be called regularly (every hour/day)
    Usage Example for the period until now:
        ```py
        # call it every hour
        tweets_start_time = datetime.datetime.now() - datetime.timedelta(hours=12)
        comments_start_time = datetime.datetime.now() - datetime.timedelta(hours=1, seconds=30)
        end_time = datetime.datetime.now() - datetime.timedelta(seconds=30)
        start_time = datetime(year=2022, month=6, day=11, hour=1, minute=0, second=1)               # start date
        get_tweets = TweetRetriever(TARGET_ACCOUNTS, API_KEY, API_SECRET_KEY,                       # instantiate object
                                    ACCESS_TOKEN, ACCESS_TOKEN_SECRET,BEARER_TOKEN)
        get_tweets.extract_tweets(tweets_start_time, comments_start_time, end_time,                 # extract tweets
                                    include_comments=True)    # extract tweets
        ```
    """
    dateformat_ = '%Y-%m-%dT%H:%M:%SZ'

    REQUEST_LIMIT = 450                                                      # per 15 min
    _REQUEST_COUNT = 0                                                       # resets to 0 every 450 requests
    _TOTAL_REQUESTS = 0                                                      # total number of requests
    _TWEETS_EXTRACTED = 0                                                    # total number of tweets/comments extracted
    _GRAND_TOTAL = 0                                                         # total number per instance

    def __init__(self, target_accounts, api_key, api_secret_key, access_token,
                 access_token_secret, bearer_token):
        """
        Constructor
        :param target_accounts: list of targets' handles
        :param api_key: str, api key
        :param api_secret_key: str, api secret key
        :param access_token: str, access token
        :param access_token_secret: str, access token secret
        :param bearer_token: str, bearer token
        """
        self.target_accounts = target_accounts
        self.API_KEY = api_key
        self.API_SECRET_KEY = api_secret_key
        self.ACCESS_TOKEN = access_token
        self.ACCESS_TOKEN_SECRET = access_token_secret
        self.BEARER_TOKEN = bearer_token

    def connect(self):
        """Connects to tweepy api"""
        auth = tweepy.OAuthHandler(self.API_KEY, self.API_SECRET_KEY)
        auth.set_access_token(self.ACCESS_TOKEN, self.ACCESS_TOKEN_SECRET)             # READ & WRITE Permissions
        return tweepy.API(auth)

    @staticmethod
    def connection_is_verified(api):
        """Verifying twitter api connection"""
        print('Verifying connection...')
        try:
            api.verify_credentials()
            print('Success! Connection is verified.')
            return True
        except TweepyException as err:
            return Exception(f'Connection is not verified: {err} Aborting...')

    @staticmethod
    def get_target_id(target_handle, api):
        """Retrieving user's id."""
        print(f'Looking up {target_handle}')
        user = None
        try:
            user = api.get_user(screen_name=target_handle)
        except NotFound:
            print(f'Handle "{target_handle}" was not found. Moving onto next one.')
        if user:
            return user.id
        return

    def get_tweets(self, user_id, start_time, end_time, next_token=None):
        """Endpoint for retrieving tweets from user's timeline."""
        url = f'https://api.twitter.com/2/users/{user_id}/tweets?'
        params = {
            'start_time': start_time,
            'end_time': end_time,                                        # defaults to now() - 30s
            'expansions': 'author_id',
            'tweet.fields': 'author_id,created_at,conversation_id,in_reply_to_user_id,text,public_metrics',
            'user.fields': 'name,username,created_at,description,verified,public_metrics',
            # 'exclude': 'retweets,replies',
            'max_results': 100,
        }
        headers = {
            'Authorization': f'Bearer {self.BEARER_TOKEN}'
        }
        if next_token:
            params['pagination_token'] = next_token
        response = requests.get(url=url, params=params, headers=headers)
        if not response:
            raise Exception(f'BAD RESPONSE: '
                            f'\nSTATUS: {response.status_code}\nMESSAGE: {response.json()}'
                            f'\nREQ URL: {response.url}\nNEXT_TOKE: {next_token}')
        return response

    def get_replies_from_tweet(self, tweet_id, start_time, end_time, next_token=None):
        """Endpoint for retrieving comments. Valid only for last 7 days!"""
        url = 'https://api.twitter.com/2/tweets/search/recent?'
        params = {
            'query': f'conversation_id:{tweet_id}',
            'start_time': start_time,
            'end_time': end_time,                                        # defaults to now() - 30s
            'expansions': 'author_id',
            'tweet.fields': 'in_reply_to_user_id,author_id,created_at,conversation_id,public_metrics',
            'user.fields': 'name,username,created_at,description,verified,public_metrics',
            'max_results': 100,
        }
        headers = {
            'Authorization': f'Bearer {self.BEARER_TOKEN}',
        }
        if next_token:
            params['pagination_token'] = next_token
        response = requests.get(url=url, params=params, headers=headers)
        if not response:
            raise Exception(f'BAD RESPONSE: '
                            f'\nSTATUS: {response.status_code}\nMESSAGE: {response.json()}'
                            f'\nREQ URL: {response.url}\nNEXT_TOKE: {next_token}')
        return response

    @staticmethod
    def response_is_empty(response):
        """Checks if response is empty"""
        empty = False
        if response.json().get('meta', {}).get('result_count') == 0:
            print('RESPONSE IS EMPTY')
            empty = True
        return empty

    @staticmethod
    def too_many_requests(response):
        """Checks if response is bad (when you hit "Too many requests" Twitter's response is still 200.)"""
        too_many = False
        if response.json().get('title') == 'Too Many Requests':
            print('Too Many Requests ERROR. Sleeping for 20 min...')
            too_many = True
        return too_many

    def reached_limit(self):
        """Checks if request limit has been reached"""
        reached = False
        if self._REQUEST_COUNT >= self.REQUEST_LIMIT - 1:                # -1 just to be safe
            reached = True
            print(f'Reached {self.REQUEST_LIMIT}, sleeping for 16 min')
            self._REQUEST_COUNT = 0
        return reached

    @staticmethod
    def parse_response(response):
        """parses response of the retrieved tweets"""
        tweets_data = response.get('data')
        tweets_dict = {}
        for tweet in tweets_data:
            info = {
                'tweet_created': tweet.get('created_at'),
                'conversation_id': tweet.get('conversation_id'),
                'tweet_id': tweet.get('id'),
                'author_id': tweet.get('author_id'),
                'text': tweet.get('text'),
                'retweet_count': tweet.get('public_metrics').get('retweet_count'),
                'reply_count': tweet.get('public_metrics').get('reply_count'),
                'like_count': tweet.get('public_metrics').get('like_count'),
                'quote_count': tweet.get('public_metrics').get('quote_count')
            }
            tweets_dict[tweet.get('id')] = info

        users_data = response.get('includes').get('users')
        users_dict = {}
        for user in users_data:
            info = {
                'account_created': user.get('created_at'),
                'account_id': user.get('id'),
                'name': user.get('name'),
                'verified': user.get('verified'),
                'follower_count': user.get('public_metrics').get('followers_count'),
                'following_count': user.get('public_metrics').get('following_count'),
                'tweet_count': user.get('public_metrics').get('tweet_count'),
                'listed_count': user.get('public_metrics').get('listed_count')
            }
            users_dict[info.get('account_id')] = info  # we do not want duplicates => key(author_id) might not be unique
        meta = response.get('meta', {})
        return tweets_dict, users_dict, meta

    @staticmethod
    def _insert_to_db(parsed_response, users_query=True):
        """
        Creates list of tuples and inserts it to database.
        NOTE: users' information has to be inserted to db first due to constraints!!!

        Parameters
        :param parsed_response: dict, parsed response
        :param users_query: bool, to pick correct query
        """
        query = QUERIES['user_info']['upsert'] if users_query else QUERIES['raw_tweets_info']['upsert']
        rows_to_insert = []
        for tweet in parsed_response.values():
            row = tuple(tweet.values())
            rows_to_insert.append(row)
        insert_to_db(rows_to_insert, query=query)

    def _extract_comments(self, conversation_id, start_time, end_time):
        """
        Takes a  tweet's ids and extracts all comments from it
        :param conversation_id: int, conversation id
        """
        print(f'\n\nExtracting comments from Conversation ID: {conversation_id}.')
        comments_per_tweet = 0
        next_token = None
        while True:
            print(f'\n\nTotal Request Count: {self._TOTAL_REQUESTS}')
            print(f'Request Count: {self._REQUEST_COUNT}')

            if self.reached_limit():
                time.sleep(60*16)

            target_response = self.get_replies_from_tweet(conversation_id, start_time, end_time, next_token=next_token)
            self._REQUEST_COUNT += 1
            self._TOTAL_REQUESTS += 1

            if self.too_many_requests(target_response):
                time.sleep(60*20)
                continue
            if self.response_is_empty(target_response):
                break

            print(f'RESPONSE STATUS: {target_response.status_code}')
            tweets_dict, user_dict, meta = self.parse_response(target_response.json())
            self._insert_to_db(user_dict, users_query=True)                             # IMPORTANT: insert users first!
            self._insert_to_db(tweets_dict, users_query=False)

            self._TWEETS_EXTRACTED += int(meta.get('result_count'))
            comments_per_tweet += int(meta.get('result_count'))

            print(f'Total number of tweets/comments extracted: {self._TWEETS_EXTRACTED}')
            next_token = meta.get('next_token', None)
            if next_token is None:
                print(f'All comments ({comments_per_tweet}) from the tweet have been extracted.')
                break

    def extract_tweets(self, tw_start_time, cm_start_time, end_time, include_comments=True):
        """
        Extracts tweets and comments from the target list up to specified date.
        NOTE: comments could be extracted only up last 7 days, unless you have premium api. However, for tweet only 3200
        most recent tweets could be retrieved
        :param tw_start_time: datetime.datetime, start time fot tweet search
        :param cm_start_time: datetime.datetime, start time fot comments search
        :param end_time: datetime.datetime, end_time for the search
        :param include_comments: bool, True to include comment search, False otherwise
        :return:
        """
        # Reset Counters & Settings
        self._REQUEST_COUNT = 0
        self._TOTAL_REQUESTS = 0
        self._TWEETS_EXTRACTED = 0
        tw_start_time = tw_start_time.strftime(self.dateformat_)
        cm_start_time = cm_start_time.strftime(self.dateformat_)
        end_time = end_time.strftime(self.dateformat_)

        api = self.connect()
        self.connection_is_verified(api)
        for target_handle in self.target_accounts:
            print(f'\n\n\nExtracting tweets from {target_handle}')

            target_id = self.get_target_id(target_handle, api)
            tweets_per_handle = 0
            next_token = None
            while True:
                print(f'\n\nTotal Request Count: {self._TOTAL_REQUESTS}')
                print(f'Request Count: {self._REQUEST_COUNT}')

                if self.reached_limit():
                    time.sleep(60*16)

                target_response = self.get_tweets(target_id, tw_start_time, end_time, next_token=next_token)
                self._REQUEST_COUNT += 1
                self._TOTAL_REQUESTS += 1

                if self.too_many_requests(target_response):
                    time.sleep(60*20)
                    continue
                if self.response_is_empty(target_response):
                    break

                print(f'FROM: {target_handle}')
                print(f'RESPONSE STATUS: {target_response.status_code}')

                tweets_dict, users_dict, meta = self.parse_response(target_response.json())
                self._insert_to_db(users_dict, users_query=True)                        # IMPORTANT: insert users first!
                self._insert_to_db(tweets_dict, users_query=False)

                self._TWEETS_EXTRACTED += int(meta.get('result_count'))
                tweets_per_handle += int(meta.get('result_count'))
                print(f'Tweets already extracted: {self._TWEETS_EXTRACTED}')
                if include_comments:
                    for tweet_info in tweets_dict.values():                         # passing tweets to extract comments
                        conversation_id = tweet_info.get('conversation_id', None)
                        if conversation_id:
                            self._extract_comments(conversation_id, cm_start_time, end_time)
                        else:
                            raise Exception('ERROR: could not find conversation in parsed data.')

                next_token = meta.get('next_token', None)
                if next_token is None:
                    print(f'All tweets ({tweets_per_handle}) from {target_handle} have been extracted.')
                    break
        db_count = retrieve_data('SELECT COUNT(DISTINCT tweet_id) FROM raw_tweets_info')[0][0]
        self._GRAND_TOTAL += self._TWEETS_EXTRACTED
        print(f'Process finished.\nTOTAL NUMBER OF EXTRACTED TWEETS THIS SESSION: {self._TWEETS_EXTRACTED}\n\n'
              f'GRAND TOTAL: {self._GRAND_TOTAL}\nEFFICIENCY: {db_count/self._GRAND_TOTAL}%\n\n')


class BtcExtractorYahoo:
    """
    Extracts BTC prices from yahoo for given period and interval, writes them to db
    Example:
    ```py
        # Daily
        btc_extractor = BtcExtractorYahoo(period='1d', interval='1d')
        btc_extractor.extract_btc()
        # Hourly
        btc_extractor = BtcExtractorYahoo(period='1h', interval='1h')
        btc_extractor.extract_btc()
    ```
    NOTE: HOURLY is not implemented because it does not work properly with Yahoo!
    Parameters
    :param period: str ['1d', '1h']
    :param interval: str ['1d', '1h']
    """
    def __init__(self, period='1d', interval='1d'):
        self.period = period
        self.interval = interval
        self._daily = True

    @staticmethod
    def parse_response(response: pd.DataFrame):
        rows = response.loc[:, :].values.tolist()
        rows_to_insert = [tuple(row) for row in rows]
        return rows_to_insert

    @staticmethod
    def _insert_to_db(rows_to_insert, daily=True):
        query = QUERIES['btc_daily_info']['upsert'] if daily else QUERIES['btc_hourly_info']['upsert']
        insert_to_db(rows_to_insert, query=query)
        print(f'{len(rows_to_insert)} have been inserted to btc_daily')

    def extract_btc(self):
        print(f'Extracting BTC for the period {self.period} with interval of {self.interval}.\n')
        btc_daily = yf.download(tickers='BTC-USD', period=self.period, interval=self.interval)
        btc_daily.reset_index(inplace=True)
        rows_to_insert = self.parse_response(btc_daily)
        self._insert_to_db(rows_to_insert, daily=self._daily)
        print('Process Finished')


class BtcExtractorCC:
    """
    Extracts hourly data for BTC, saves it to database.
    https://min-api.cryptocompare.com/documentation
    API allows up to 100,000 free calls per month.
    Class should not make more calls than (31 days * 24 hours) = 720
    """
    def __init__(self, api_key, frequency='hourly'):
        """
        Constructor
        :param api_key: str, api key from CompareCrypto (it's free)
        :param frequency: str, ['hourly', 'daily'] (daily is not configured)
        """
        self.api_key = api_key
        self.frequency = frequency
        self.url = 'https://min-api.cryptocompare.com/data/v2/histohour?'
        self.limit = 1

    def get_request(self):
        """
        Sends and returns response.
        :return: response
        """
        params = {
            'fsym': 'BTC',                                                          # coin symbol
            'tsym': 'USD',                                                          # currency symbol to convert to
            'limit': self.limit,                                                    # number of data points
        }
        headers = {
            'accept': 'application/json',
            'authorization': f'Apikey {self.api_key}'
        }
        response = requests.get(url=self.url, headers=headers, params=params)
        if not response:
            raise Exception(f'BAD RESPONSE: '
                            f'\nSTATUS: {response.status_code}\nMESSAGE: {response.json()}'
                            f'\nREQ URL: {response.url}')
        return response

    def _insert_to_db(self, parsed_response):
        """
        Creates list of tuples and inserts it to database"
        :param parsed_response: list, parsed response
        """
        rows = [tuple(hour.values()) for hour in parsed_response]
        insert_to_db(rows, query=QUERIES['btc_hourly_info']['upsert'])
        print(f'{len(rows)} have been inserted to {self.frequency}.')

    def parse_response(self, response):
        """
        Parses response, saves response to db by calling _insert_to_db method
        :param response:
        :return:
        """
        res = response.json()
        data = res.get('Data', {}).get('Data', None)
        if data is None:
            raise Exception('Data is None')
        parsed = []
        for point in data:
            dp = {k: v for k, v in point.items() if k in ('time', 'high', 'low', 'open', 'volumefrom',
                                                          'volumeto', 'close')}
            dp['time'] = datetime.fromtimestamp(dp['time']).strftime('%Y-%m-%d %T')
            parsed.append(dp)
        self._insert_to_db(parsed)
        return parsed

    def extract_bitcoin(self):
        print(f'Requesting BitCoin {self.frequency} Data...')
        response = self.get_request()
        print(f'RESPONSE STATUS: {response.status_code}')
        self.parse_response(response)
        print(f'Process finished.\n')
