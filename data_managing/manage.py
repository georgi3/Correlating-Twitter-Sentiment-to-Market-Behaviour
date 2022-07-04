import datetime
import time
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from config import *
from data_extraction import TweetRetriever, BtcExtractorYahoo, BtcExtractorCC
from preprocessing import text_pipe
from db_handler import insert_to_db, retrieve_data, create_table, create_connection, QUERIES

# CHECK DB CONNECTION
if create_connection():
    print('Successful Connection to DataBase!')
else:
    raise Exception('ERROR: Could NOT connect to the database!')

# CREATE TABLES
for table in QUERIES.values():
    create_table(table['create_table'])


def preprocess_extracted_data(start_time):
    """
    Preprocesses tweets that were created last hour, writes them to db
    :param start_time:  datetime.datetime, preprocessed tweets that were created after start_time
    """
    print('Preprocessing Newly Extracted Data')
    query = f"""
    SELECT
        tweet_created,
        conversation_id,
        tweet_id,
        author_id,
        tweet_text
    FROM raw_tweets_info
    WHERE tweet_created >= '{start_time.strftime('%Y-%m-%d %T')}'::timestamp;
    """
    df = pd.DataFrame(retrieve_data(query=query), columns=['tweet_created', 'conversation_id', 'tweet_id',
                                                           'author_id', 'text'])
    df = text_pipe.fit_transform(df)
    rows_to_insert = [tuple(row) for row in df.loc[:, :].values.tolist()]
    insert_to_db(rows_to_insert, query=QUERIES['preprocessed_tweets_info']['upsert'])
    print('Preprocessing is finished...')


def extract_tweets_hourly(extractor):
    """
    Extracts raw tweets => raw_tweets_info db;
    Preprocesses raw tweets => preprocessed_tweets_info db
    :param extractor: instance of TweetRetriever
    :return:
    """
    end_time = datetime.datetime.now() - datetime.timedelta(seconds=30)                            # -30 for Twitter API
    comments_start_time = datetime.datetime.now() - datetime.timedelta(hours=1, seconds=30)        # -30 to match up
    tweets_start_time = datetime.datetime.now() - datetime.timedelta(hours=12)
    print(f'\n\nEXTRACTING TWEETS\nCM_START_TIME {comments_start_time.strftime("%Y-%m-%d %T")}')
    extractor.extract_tweets(tweets_start_time, comments_start_time, end_time, include_comments=True)
    # PREPROCESS
    preprocess_extracted_data(comments_start_time)


def extract_btc_daily(extractor):
    """
    Extracts daily stats for bitcoin
    :param extractor: BtcExtractorYahoo instance
    """
    print('\n\nEXTRACTING BTC DAILY')
    extractor.extract_btc()


def extract_btc_hourly(extractor):
    """
    Extracts hourly stats for bitcoin
    :param extractor: BtcExtractorCC instance
    :return:
    """
    print('\n\nEXTRACTING BTC HOURLY')
    extractor.extract_bitcoin()


if __name__ == '__main__':
    tweet_extractor = TweetRetriever(TARGET_ACCOUNTS, API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET,
                                     BEARER_TOKEN)
    daily_btc_extractor = BtcExtractorYahoo(period='2d', interval='1d')
    hourly_btc_extractor = BtcExtractorCC(CC_API_KEY)
    scheduler = BackgroundScheduler(timezone='US/Eastern')
    scheduler.add_job(extract_tweets_hourly, 'interval', hours=1, kwargs={'extractor': tweet_extractor},
                      next_run_time=datetime.datetime.now())
    scheduler.add_job(extract_btc_hourly, 'interval', hours=1, kwargs={'extractor': hourly_btc_extractor},
                      next_run_time=datetime.datetime.now())
    scheduler.add_job(extract_btc_daily, 'interval', hours=24, kwargs={'extractor': daily_btc_extractor},
                      next_run_time=datetime.datetime.now())
    scheduler.start()
    print(F'Press Ctrl+C to exit')

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
