import time
import tweepy
import requests
from datetime import datetime, timedelta
from tweepy.errors import TweepyException, NotFound
from src.db_handler import insert_to_db


class BtcHourly:
    """
    Extracts hourly data for BTC, saves it to database.

    API allows up to 100,000 free calls per month.
    Class should not make more calls than (31 days * 24 hours) = 720
    """
    def __init__(self, api_key, frequency=24):
        """
        Constructor
        :param api_key: str, api key from CompareCrypto (it's free)
        :param frequency: int,  data for past x hours (max 2000)
        """
        self.api_key = api_key
        self.frequency = frequency

    def get_request(self):
        """
        Sends and returns response.
        :return: response
        """
        url = 'https://min-api.cryptocompare.com/data/v2/histohour?'                # CompareCrypto
        params = {
            'fsym': 'BTC',                                                          # coin symbol
            'tsym': 'USD',                                                          # currency symbol to convert to
            'limit': self.frequency,                                                # number of data points
        }
        headers = {
            'accept': 'application/json',
            'authorization': f'Apikey {self.api_key}'
        }
        response = requests.get(url=url, headers=headers, params=params)
        if not response:
            raise Exception(f'BAD RESPONSE: '
                            f'\nSTATUS: {response.status_code}\nMESSAGE: {response.json()}'
                            f'\nREQ URL: {response.url}')
        return response

    @staticmethod
    def _insert_to_db(parsed_response):
        """
        Creates list of tuples and inserts it to database"
        :param parsed_response: list, parsed response
        """""
        query = """
            INSERT INTO btc_hourly 
            (
            TIME_STAMP, HIGH, LOW, OPEN_, CLOSE_, VOLUME_FROM, VOLUME_TO
            ) VALUES %s;
        """
        rows = [tuple(hour.values()) for hour in parsed_response]
        insert_to_db(rows, query=query)
        print(f'{len(rows)} have been inserted to btc_hourly.')

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
            parsed.append(dp)
        self._insert_to_db(parsed)
        return parsed

    def extract_bitcoin_hourly(self):
        print('Requesting BitCoin Hourly Data...')
        response = self.get_request()
        print(f'RESPONSE STATUS: {response.status_code}')
        self.parse_response(response)
        print(f'Process finished.\n')


class TweetRetriever:
    """
    # https://developer.twitter.com/en/docs/twitter-api/rate-limits
    Retrieves tweets and their comments from the list of user accounts; saves tweets to the database.
        NOTE: comments could be extracted only up last 7 days, unless you have premium api. However, for tweets you can
        extract up to 3200 most recent tweets from user's timeline. For more check twitter's api.

    Usage Example for the period until now:
        ```py
        start_time = datetime(year=2022, month=6, day=11, hour=1, minute=0, second=1)   # start date
        get_tweets = TweetRetriever(TARGET_ACCOUNTS, API_KEY,                           # instantiate object
                                    API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET,
                                     BEARER_TOKEN, start_time)
        get_tweets.extract_tweets(include_comments=True)                                # extract tweets
        ```
    Usage Example for the period in the past:
        ```py
        dateformat_ = '%Y-%m-%dT%H:%M:%SZ'
        start_time = datetime(year=2022, month=6, day=11, hour=1, minute=0, second=1)   # start time
        end_time = datetime(year=2022, month=6, day=12, hour=1, minute=0, second=0)     # end time
        end_time = end_time.strftime(dateformat_)                                       # convert to string (only end)

        get_tweets = TweetRetriever(TARGET_ACCOUNTS, API_KEY,
                                    API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET,  # instantiate object
                                     BEARER_TOKEN, start_time)
        get_tweets.END_TIME = end_time                                                  # set end time
        get_tweets.extract_tweets(include_comments=True)                                # extract tweets
    """
    dateformat_ = '%Y-%m-%dT%H:%M:%SZ'
    end_time_ = datetime.now() - timedelta(seconds=60)
    END_TIME = end_time_.strftime(dateformat_)                               # Retrieve tweets up to last 60 sec

    REQUEST_LIMIT = 450                                                      # per 15 min
    _REQUEST_COUNT = 0                                                       # resets to 0 every 450 requests
    _TOTAL_REQUESTS = 0                                                      # total number of requests
    _TWEETS_EXTRACTED = 0                                                    # total number of tweets/comments extracted

    def __init__(self, target_accounts, api_key, api_secret_key,
                 access_token, access_token_secret, bearer_token, period):
        """
        Constructor
        :param target_accounts: list of targets' handles
        :param api_key: str, api key
        :param api_secret_key: str, api secret key
        :param access_token: str, access token
        :param access_token_secret: str, access token secret
        :param bearer_token: str, bearer token
        :param period: datetime.datetime obj, searches tweets up to this date
        """
        self.target_accounts = target_accounts
        self.API_KEY = api_key
        self.API_SECRET_KEY = api_secret_key
        self.ACCESS_TOKEN = access_token
        self.ACCESS_TOKEN_SECRET = access_token_secret
        self.BEARER_TOKEN = bearer_token
        self.START_TIME = period.strftime(self.dateformat_)
        self.query_year = period.year
        self.query_month = period.month
        self.query_day = period.day
        self.query_hour = period.hour

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

    def get_tweets(self, user_id, next_token=None):
        """Endpoint for retrieving tweets from user's timeline."""
        url = f'https://api.twitter.com/2/users/{user_id}/tweets?'
        params = {
            'start_time': self.START_TIME,
            'end_time': self.END_TIME,
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

    def get_replies_from_tweet(self, tweet_id, next_token=None):
        """Endpoint for retrieving comments. Valid only for last 7 days!"""
        url = 'https://api.twitter.com/2/tweets/search/recent?'
        params = {
            'query': f'conversation_id:{tweet_id}',
            'end_time': self.END_TIME,
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

    def parse_response(self, response):
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
            users_dict[user.get('id')] = info

        parsed_data = self.combine_tweet_user_data(tweets_dict, users_dict)
        meta = response.get('meta', {})
        return parsed_data, meta

    def combine_tweet_user_data(self, tweets, users):
        """
        Combines tweets dictionary with authors dictionary
        :param tweets: dict parsed tweets dictionary
        :param users: dict parsed users dictionary
        :return: dict combined dictionary with query params included
        """
        combined = {}
        extraction_info = {
            'QUERY_YEAR': self.query_year,
            'QUERY_MONTH': self.query_month,
            'QUERY_DAY': self.query_day,
            'QUERY_HOUR': self.query_hour,
        }
        for tweet_id, tweet_info in tweets.items():
            user_info = users[tweet_info['author_id']]
            combined_info = {**extraction_info, **tweet_info, **user_info}
            combined[tweet_id] = combined_info
        return combined

    @staticmethod
    def _insert_to_db(parsed_response):
        """
        Creates list of tuples and inserts it to database"
        :param parsed_response: dict, parsed response
        """""
        rows_to_insert = []
        for tweet in parsed_response.values():
            row = tuple(tweet.values())
            rows_to_insert.append(row)
        insert_to_db(rows_to_insert)

    def _extract_comments(self, conversation_id):
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

            target_response = self.get_replies_from_tweet(conversation_id, next_token=next_token)
            self._REQUEST_COUNT += 1
            self._TOTAL_REQUESTS += 1

            if self.too_many_requests(target_response):
                time.sleep(60*20)
                continue
            if self.response_is_empty(target_response):
                break

            print(f'RESPONSE STATUS: {target_response.status_code}')
            parsed_data, meta = self.parse_response(target_response.json())
            self._insert_to_db(parsed_data)

            self._TWEETS_EXTRACTED += int(meta.get('result_count'))
            comments_per_tweet += int(meta.get('result_count'))

            print(f'Total number of tweets/comments extracted: {self._TWEETS_EXTRACTED}')
            next_token = meta.get('next_token', None)
            if next_token is None:
                print(f'All comments ({comments_per_tweet}) from the tweet have been extracted.')
                break

    def extract_tweets(self, include_comments=True):
        """
        Extracts tweets and comments from the target list up to specified date.
        NOTE: comments could be extracted only up last 7 days, unless you have premium api. However, for tweet only 3200
        most recent tweets could be retrieved.
        """
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

                target_response = self.get_tweets(target_id, next_token=next_token)
                self._REQUEST_COUNT += 1
                self._TOTAL_REQUESTS += 1

                if self.too_many_requests(target_response):
                    time.sleep(60*20)
                    continue
                if self.response_is_empty(target_response):
                    break

                print(f'FROM: {target_handle}')
                print(f'RESPONSE STATUS: {target_response.status_code}')

                parsed_data, meta = self.parse_response(target_response.json())
                self._insert_to_db(parsed_data)

                self._TWEETS_EXTRACTED += int(meta.get('result_count'))
                tweets_per_handle += int(meta.get('result_count'))
                print(f'Tweets already extracted: {self._TWEETS_EXTRACTED}')
                if include_comments:
                    for tweet_info in parsed_data.values():                         # passing tweets to extract comments
                        conversation_id = tweet_info.get('conversation_id', None)
                        if conversation_id:
                            self._extract_comments(conversation_id)
                        else:
                            raise Exception('ERROR: could not find conversation in parsed data.')

                next_token = meta.get('next_token', None)
                if next_token is None:
                    print(f'All tweets ({tweets_per_handle}) between {self.START_TIME}'
                          f' and {self.END_TIME} from {target_handle} have been extracted.')
                    break
        print(f'Process finished.\nTOTAL NUMBER OF EXTRACTED TWEETS: {self._TWEETS_EXTRACTED}')
