import psycopg2
from psycopg2 import DatabaseError
from psycopg2.extras import execute_values
from src.config import USER, DATABASE, PASSWORD, PORT, HOST


def create_connection():
    """Connects to db server"""
    try:
        connection = psycopg2.connect(user=USER, database=DATABASE, host=HOST, port=PORT, password=PASSWORD)
        cursor = connection.cursor()
    except (Exception, DatabaseError) as err:
        raise Exception(f'Could not connect to server: {err}')
    return connection, cursor


def create_table():
    """Creates sql table"""
    query = f"""
    CREATE TABLE IF NOT EXISTS  twitter_btc
    (
        PK_ID SERIAL PRIMARY KEY ,
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
        LIST_COUNT INTEGER
    );
    """
    connection = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query)
        connection.commit()
        cursor.close()
        print('Table has been successfully created!')
    except (Exception, DatabaseError) as err:
        print(f'Failed to create table: {err}')
    finally:
        if connection is not None:
            connection.close()


def insert_to_db(values_list: list):
    """
    Inserts rows into table.
    :param values_list: list, list of tuples separates by coma, e.g. [(1, 3, ..., 2), (2, 1, ... 3)]
    """

    query = f"""
    INSERT INTO twitter_btc
    (
        QUERY_YEAR,
        QUERY_MONTH,
        QUERY_DAY,
        QUERY_HOUR,
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
        LIST_COUNT
    )
    VALUES %s;
"""
    connection = None
    try:
        connection, cursor = create_connection()
        execute_values(cursor, query, values_list)
        connection.commit()
        cursor.close()
        print('Data has been successfully inserted!')
    except (Exception, DatabaseError) as err:
        raise Exception(f'Failed to insert rows! ERROR: {err}')
    finally:
        if connection is not None:
            connection.close()


def _delete_all_data_from_table(table: str):
    """Deletes all data from the table"""
    query = f"""DELETE FROM {table};"""
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


def retrieve_all_data(table='twitter_btc'):
    """Retrieves all the data"""
    query = f"""SELECT TWEET_CREATED, CONVERSATION_ID, TWEET_ID, AUTHOR_ID, TWEET_TEXT, RETWEET_COUNT, REPLY_COUNT,
     LIKE_COUNT, QUOTE_COUNT, ACCOUNT_CREATED, ACCOUNT_ID, ACCOUNT_NAME, VERIFIED, FOLLOWER_COUNT, FOLLOWING_COUNT,
      TWEET_COUNT, LIST_COUNT FROM {table};"""
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


def retrieve_from_last_period(period):
    """
    Retrieves data for the last period.
    :param period: datetime.datetime object
    :return: list of rows
    """
    year, month, day, hour = period.year, period.month, period.day, period.hour

    query = f"""SELECT TWEET_CREATED, CONVERSATION_ID, TWEET_ID, AUTHOR_ID, TWEET_TEXT, RETWEET_COUNT, REPLY_COUNT,
     LIKE_COUNT, QUOTE_COUNT, ACCOUNT_CREATED, ACCOUNT_ID, ACCOUNT_NAME, VERIFIED, FOLLOWER_COUNT, FOLLOWING_COUNT,
     TWEET_COUNT, LIST_COUNT FROM twitter_btc
      WHERE QUERY_YEAR >= %s AND QUERY_MONTH >= %s AND QUERY_DAY >= %s AND QUERY_HOUR >= %s;"""
    connection = None
    rows = None
    try:
        connection, cursor = create_connection()
        cursor.execute(query, (year, month, day, hour))
        rows = cursor.fetchall()
        cursor.close()
        print(f'Rows have been retrieved')
    except (Exception, DatabaseError) as err:
        print(f'Failed to retrieve rows. ERROR: {err}')
    finally:
        connection.close()
    return rows
