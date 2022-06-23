from db_handler import create_table

query_btc_hourly = """
CREATE TABLE IF NOT EXISTS  btc_hourly
(
    PK_ID SERIAL PRIMARY KEY,
    TIME_STAMP TEXT,
    HIGH FLOAT,
    LOW FLOAT,
    OPEN_ FLOAT,
    VOLUME_FROM FLOAT,
    VOLUME_TO FLOAT,
    CLOSE_ FLOAT
);
"""
query_btc_daily = """
CREATE TABLE IF NOT EXISTS  btc_daily
(
    PK_ID SERIAL PRIMARY KEY,
    TIME_STAMP TEXT,
    HIGH FLOAT,
    LOW FLOAT,
    OPEN_ FLOAT,
    VOLUME_FROM FLOAT,
    VOLUME_TO FLOAT,
    CLOSE_ FLOAT
);"""

query_raw_tweets = """
    CREATE TABLE IF NOT EXISTS  raw_tweets
    (
        PK_ID SERIAL PRIMARY KEY,
        QUERY_YEAR INTEGER,
        QUERY_MONTH INTEGER,
        QUERY_DAY INTEGER,
        QUERY_HOUR INTEGER,
        TWEET_CREATED TEXT,
        CONVERSATION_ID TEXT,
        TWEET_ID TEXT NOT NULL,
        AUTHOR_ID TEXT,
        TWEET_TEXT TEXT,
        RETWEET_COUNT INTEGER,
        REPLY_COUNT INTEGER,
        LIKE_COUNT INTEGER,
        QUOTE_COUNT INTEGER,
        ACCOUNT_CREATED TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT_NAME TEXT,
        VERIFIED TEXT,
        FOLLOWER_COUNT INTEGER,
        FOLLOWING_COUNT INTEGER,
        TWEET_COUNT INTEGER,
        LISTED_COUNT INTEGER
    );
    """

query_preprocessed = """
    CREATE TABLE IF NOT EXISTS  preprocessed_tweets
    (
        PK_ID SERIAL PRIMARY KEY,
        TWEET_CREATED TEXT,
        CONVERSATION_ID TEXT,
        TWEET_ID TEXT NOT NULL,
        AUTHOR_ID TEXT,
        TWEET_TEXT TEXT,
        RETWEET_COUNT INTEGER,
        REPLY_COUNT INTEGER,
        LIKE_COUNT INTEGER,
        QUOTE_COUNT INTEGER,
        ACCOUNT_CREATED TEXT,
        ACCOUNT_ID TEXT,
        ACCOUNT_NAME TEXT,
        VERIFIED TEXT,
        FOLLOWER_COUNT INTEGER,
        FOLLOWING_COUNT INTEGER,
        TWEET_COUNT INTEGER,
        LISTED_COUNT INTEGER,
        WORD_UNIQUENESS INTEGER,
        SENTIMENT FLOAT
    );
    """

tables = query_preprocessed, query_raw_tweets, query_btc_hourly, query_btc_daily
for query in tables:
    create_table(query)


