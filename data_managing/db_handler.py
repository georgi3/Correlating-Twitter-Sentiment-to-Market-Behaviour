import psycopg2
from psycopg2 import DatabaseError
from psycopg2.extras import execute_values
from config import USER, DATABASE, PASSWORD, PORT, HOST


def create_connection():
    """Connects to db server"""
    try:
        connection = psycopg2.connect(user=USER, database=DATABASE, host=HOST, port=PORT, password=PASSWORD,
                                      sslmode='require')
        cursor = connection.cursor()
    except (Exception, DatabaseError) as err:
        raise Exception(f'Could not connect to server: {err}')
    return connection, cursor


def create_table(query):
    """Creates sql table"""
    connection = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print(f'Table "{query.split()[5]}" has been successfully created!')
    except (Exception, DatabaseError) as err:
        print(f'Failed to create table: {err}')
    finally:
        if connection is not None:
            connection.close()


def insert_to_db(values_list: list, query: str):
    """
    Inserts rows into a table
    :param values_list: list, list of tuples separates by coma, e.g. [(1, 3, ..., 2), (2, 1, ... 3)]
    :param query: str, query
    """
    connection = None
    try:
        connection, cursor = create_connection()
        execute_values(cursor, query, values_list)
        connection.commit()
        cursor.close()
        print(f'Data has been successfully inserted to {query.split()[2]}')
    except (Exception, DatabaseError) as err:
        raise Exception(f'Failed to insert rows! ERROR: {err}')
    finally:
        if connection is not None:
            connection.close()


def _delete_all_data_from_table(table: str, query=None):
    """Deletes all data from the table"""
    query = query if query else f"""DELETE FROM {table};"""
    connection = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print('All data has been deleted from the table.')
    except (Exception, DatabaseError) as err:
        print(f'Failed to delete table {table}. ERROR: {err}')
    finally:
        connection.close()


def _delete_table(table: str):
    """Deletes passed table"""
    query = f"""DROP TABLE {table};"""
    connection = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print(f'Table "{table}" has been deleted')
    except (Exception, DatabaseError) as err:
        print(f'Failed to delete table {table}. ERROR: {err}')
    finally:
        connection.close()


def retrieve_data(query):
    """Retrieves all the data"""
    connection = None
    rows = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        print(f'Rows have been retrieved')
    except (Exception, DatabaseError) as err:
        print(f'Failed to retrieve rows. ERROR: {err}')
    finally:
        connection.close()
    return rows


# USER DB
user_info = """
CREATE TABLE IF NOT EXISTS user_info
    (
    pk_id SERIAL,
    account_created TIMESTAMP,
    account_id VARCHAR(30) UNIQUE,
    account_name VARCHAR(250),
    verified BOOLEAN,
    follower_count INTEGER,
    following_count INTEGER,
    tweet_count INTEGER,
    listed_count INTEGER,
    PRIMARY KEY(pk_id, account_id)
    );
"""
user_info_upsert = """
INSERT INTO user_info 
(account_created, account_id, account_name, verified, follower_count, following_count, tweet_count, listed_count)
    VALUES %s
    ON CONFLICT (account_id) DO UPDATE 
    SET verified = EXCLUDED.verified, 
        follower_count = EXCLUDED.follower_count,
        following_count = EXCLUDED.following_count,
        tweet_count = EXCLUDED.tweet_count,
        listed_count = EXCLUDED.listed_count;
"""
user_info_retrieve_all = """
SELECT (account_created, account_id, account_name, verified, follower_count, following_count, tweet_count, listed_count)
FROM user_info
"""

# RAW TWEETS DB
raw_tweets_info = """
CREATE TABLE IF NOT EXISTS raw_tweets_info 
    (
    pk_id SERIAL,
    tweet_created TIMESTAMP,
    conversation_id VARCHAR(30),
    tweet_id VARCHAR(30) UNIQUE,
    author_id VARCHAR(30),
    tweet_text TEXT,
    retweet_count INTEGER,
    reply_count INTEGER,
    like_count INTEGER,
    quote_count INTEGER,
    PRIMARY KEY(pk_id, tweet_id),
    CONSTRAINT fk_author_id
        FOREIGN KEY(author_id)
            REFERENCES user_info(account_id)
    );
"""
raw_tweets_upsert = """
INSERT INTO raw_tweets_info 
(tweet_created, conversation_id, tweet_id, author_id, tweet_text, retweet_count, reply_count, like_count, quote_count)
    VALUES %s
    ON CONFLICT (tweet_id) DO UPDATE
    SET retweet_count = EXCLUDED.retweet_count,
        reply_count = EXCLUDED.reply_count,
        like_count = EXCLUDED.like_count,
        quote_count = EXCLUDED.quote_count;
"""
raw_tweets_retrieve_all = """
SELECT (tweet_created, conversation_id, author_id, tweet_text, retweet_count, reply_count, like_count, quote_count)
FROM raw_tweets_info
"""

# PREPROCESSED TWEETS DB
preprocessed_tweets_info = """
CREATE TABLE IF NOT EXISTS preprocessed_tweets_info 
    (
    pk_id SERIAL,
    tweet_id VARCHAR(30) UNIQUE,
    cleaned_text TEXT,
    vader_compound FLOAT,
    text_blob_polarity FLOAT,
    text_blob_subjectivity FLOAT,
    PRIMARY KEY(pk_id),
    CONSTRAINT fk_tweet_id
        FOREIGN KEY(tweet_id)
            REFERENCES raw_tweets_info(tweet_id)
    );
"""
preprocessed_tweets_info_upsert = """
INSERT INTO preprocessed_tweets_info 
(tweet_id, cleaned_text, vader_compound, text_blob_polarity, text_blob_subjectivity)
    VALUES %s
    ON CONFLICT (tweet_id) DO NOTHING;
"""
preprocessed_tweets_info_retrieve_all = """
SELECT (tweet_id, cleaned_text, vader_compound, text_blob_polarity, text_blob_subjectivity)
FROM preprocessed_tweets_info
"""

# BTC DAILY INFO
btc_daily = """
CREATE TABLE IF NOT EXISTS btc_daily_info
    (
    pk_id SERIAL,
    input_date DATE UNIQUE,
    open_price DECIMAL, 
    high_price DECIMAL,
    low_price DECIMAL,
    close_price DECIMAL,
    adj_close_price DECIMAL,
    volume DECIMAL,
    PRIMARY KEY(pk_id)
    );
"""
btc_daily_upsert = """
INSERT INTO btc_daily_info
(input_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
    VALUES %s
    ON CONFLICT (input_date) DO NOTHING;
"""
btc_daily_retrieve_all = """
SELECT (input_date, open_price, high_price, low_price, close_price, adj_close_price, volume)
FROM btc_daily_info
"""

# BTC HOURLY INFO
btc_hourly = """
CREATE TABLE IF NOT EXISTS btc_hourly_info
    (
    pk_id SERIAL,
    input_datetime TIMESTAMP UNIQUE,
    high_price DECIMAL,
    low_price DECIMAL,
    open_price DECIMAL,
    volumefrom DECIMAL,
    volumeto DECIMAL,
    close_price DECIMAL, 
    PRIMARY KEY(pk_id)
    );
"""
btc_hourly_upsert = """
INSERT INTO btc_hourly_info
(input_datetime, high_price, low_price, open_price, volumefrom, volumeto, close_price)
    VALUES %s
    ON CONFLICT (input_datetime) DO UPDATE
    SET high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        open_price = EXCLUDED.open_price,
        volumefrom = EXCLUDED.volumefrom,
        volumeto = EXCLUDED.volumeto,
        close_price = EXCLUDED.close_price;
"""
btc_hourly_retrieve_all = """
SELECT (input_datetime, high_price, low_price, open_price, volumefrom, volumeto, close_price)
FROM btc_hourly_info
"""
QUERIES = {
    'user_info': {
        'create_table': user_info,
        'upsert': user_info_upsert,
        'retrieve_all': user_info_retrieve_all,
    },
    'raw_tweets_info': {
        'create_table': raw_tweets_info,
        'upsert': raw_tweets_upsert,
        'retrieve_all': raw_tweets_retrieve_all,
    },
    'preprocessed_tweets_info': {
        'create_table': preprocessed_tweets_info,
        'upsert': preprocessed_tweets_info_upsert,
        'retrieve_all': preprocessed_tweets_info_retrieve_all,
    },
    'btc_daily_info': {
        'create_table': btc_daily,
        'upsert': btc_daily_upsert,
        'retrieve_all': btc_hourly_retrieve_all,
    },
    'btc_hourly_info': {
        'create_table': btc_hourly,
        'upsert': btc_hourly_upsert,
        'retrieve_all': btc_hourly_retrieve_all,
    }
}