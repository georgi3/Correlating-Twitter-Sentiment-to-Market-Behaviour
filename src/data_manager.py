from datetime import datetime
import time
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from src.config import *
from src.data_extraction import TweetRetriever, BtcDailyYahoo, BtcExtractor
from src.preprocessing import text_pipe
from src.db_handler import insert_to_db, retrieve_from_last_period


preprocess_query = """
INSERT INTO preprocessed_tweets (
    TWEET_CREATED,
    CONVERSATION_ID,
    TWEET_ID,
    AUTHOR_ID,
    TWEET_TEXT,
    RETWEET_COUNT,
    REPLY_COUNT,
    LIKE_COUNT,
    QUOTE_COUNT,
    ACCOUNT_CREATED,
    ACCOUNT_ID,
    ACCOUNT_NAME,
    VERIFIED,
    FOLLOWER_COUNT,
    FOLLOWING_COUNT,
    TWEET_COUNT,
    LISTED_COUNT,
    WORD_UNIQUENESS,
    SENTIMENT
)
VALUES %s;
"""


def get_now():
    return datetime.today()


def extract_tweets_hourly():
    now = get_now()
    tweet_retriever = TweetRetriever(target_accounts=TARGET_ACCOUNTS,
                                     api_key=API_KEY, api_secret_key=API_SECRET_KEY,
                                     access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET,
                                     bearer_token=BEARER_TOKEN, period=now)
    tweet_retriever.extract_tweets(include_comments=True)

    df = pd.DataFrame(retrieve_from_last_period(now, 'raw_tweets'),
                      columns=['tweet_created', 'conversation_id', 'tweet_id', 'author_id', 'text', 'retweet_count',
                               'reply_count', 'like_count', 'quote_count', 'account_created', 'account_id', 'name',
                               'verified', 'follower_count', 'following_count', 'tweet_count', 'listed_count'])
    df = text_pipe.fit_transform(df)
    rows = df.loc[:, :]
    rows_to_insert = []
    for row in rows:
        rows_to_insert.append(tuple(row))
    insert_to_db(rows_to_insert, query=preprocess_query)


def extract_btc_daily():
    btc_daily = BtcDailyYahoo()
    btc_daily.extract_btc()


def extract_btc_hourly():
    btc_hourly = BtcExtractor(CP_API_KEY, frequency='hourly')
    btc_hourly.extract_bitcoin()


if __name__ == '__main__':

    scheduler = BackgroundScheduler()
    scheduler.add_job(extract_tweets_hourly(), 'interval', hours=1)
    scheduler.add_job(extract_btc_hourly(), 'interval', hours=1)
    scheduler.add_job(extract_btc_daily(), 'interval', hours=24)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
