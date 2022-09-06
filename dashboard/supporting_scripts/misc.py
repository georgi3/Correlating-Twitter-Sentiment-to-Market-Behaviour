import pandas as pd
from dashboard.supporting_scripts.constants import ACCOUNTS, GET_ID, query_tweets, query_btc_daily, query_btc_hourly
from data_managing.db_handler import retrieve_data
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline


def to_datetime(dataframe: pd.DataFrame, columns: list):
    """
    Converts passed columns to datetime objects
    :param dataframe: pd.DataFrame
    :param columns: list, columns to convert
    :return: pd.DatFrame
    """
    for column in columns:
        dataframe[column] = pd.to_datetime(dataframe[column])
    return dataframe


def tweet_daily_pipe(dataframe):
    """
    Preprocessing helper function for tweets for daily analysis. Function groups tweets on daily basis and calculates
    sentiment per period.
    :param dataframe: pd.DataFrame
    :return: pd.DataFrame
    """
    df = dataframe.copy(deep=True)
    df = dashboard_pipe.fit_transform(df)
    df = df.groupby([df['tweet_created'].dt.strftime('%y-%m-%d')]
                    )[['vader_compound', 'tb_polarity', 'tb_subjectivity']].agg(['mean', 'count'])
    df.drop([('tb_polarity', 'count'), ('tb_subjectivity', 'count')], axis=1, inplace=True)
    df.columns = ['avg_vader_compound', 'tweet_count', 'avg_tb_polarity', 'avg_tb_subjectivity']
    return df


def btc_daily_pipe(dataframe: pd.DataFrame):
    """
    Preprocessing helper function for BTC dataframe for daily analysis. Converts to pd.datetime
    :param dataframe: pd.DataFrame
    :return: pd.DataFrame
    """
    df = dataframe.copy(deep=True)
    df['input_date'] = pd.to_datetime(df['input_date'])
    df['date'] = df['input_date'].dt.strftime('%y-%m-%d')
    return df


def daily_pipe(tweets_daily: pd.DataFrame, btc_daily: pd.DataFrame):
    """
    Preprocessing steps for daily analysis. Merges preprocessed BTC dataframe with preprocessed tweets dataframe,
    normalizes some measurements.
    :param tweets_daily: pd.DataFrame, tweets dataframe
    :param btc_daily: pd.DataFrame, btc dataframe
    :return: pd.DataFrame
    """
    tweets_daily_df = tweets_daily.copy(deep=True)
    btc_daily_df = btc_daily.copy(deep=True)
    btc_daily_df = btc_daily_pipe(btc_daily_df)
    tweets_daily_df = tweet_daily_pipe(tweets_daily_df)
    dataframe = pd.merge(btc_daily_df, tweets_daily_df, left_on='date', right_index=True, how='right')
    dataframe.set_index('input_date', inplace=True)
    dataframe.dropna(how='any', axis=0, inplace=True)  # drops the last date for which tweets exist but BTC stats don't
    dataframe[['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']] \
        = dataframe[['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']].astype(int)
    scaler = MinMaxScaler()
    dataframe[['open_price_norm', 'high_price_norm', 'low_price_norm', 'close_price_norm', 'avg_vader_compound_norm',
               'avg_tb_polarity_norm', 'avg_tb_subjectivity_norm', 'volume_norm', 'tweet_count_norm']] = \
        scaler.fit_transform(dataframe[['open_price', 'high_price', 'low_price', 'close_price', 'avg_vader_compound',
                                        'avg_tb_polarity', 'avg_tb_subjectivity', 'volume', 'tweet_count']])
    return dataframe


def tweet_hourly_pipe(dataframe: pd.DataFrame):
    """
    Preprocessing helper function for tweets for hourly analysis. Function groups tweets on hourly basis and calculates
    sentiment per period.
    :param dataframe: pd.DataFrame
    :return: pd.DataFrame
    """
    df = dataframe.copy(deep=True)
    df = dashboard_pipe.fit_transform(df)
    df = df.groupby([df['tweet_created'].dt.strftime('%y-%m-%d-%H')]
                    )[['vader_compound', 'tb_polarity', 'tb_subjectivity']].agg(['mean', 'count'])
    df.drop([('tb_polarity', 'count'), ('tb_subjectivity', 'count')], axis=1, inplace=True)
    df.columns = ['avg_vader_compound', 'tweet_count', 'avg_tb_polarity', 'avg_tb_subjectivity']
    return df


def btc_hourly_pipe(dataframe):
    """
    Preprocessing helper function for BTC dataframe for hourly analysis. Converts to pd.datetime
    :param dataframe: pd.DataFrame
    :return: pd.DataFrame
    """
    df = dataframe.copy(deep=True)
    dataframe['input_datetime'] = pd.to_datetime(dataframe['input_datetime'])
    df['date'] = df['input_datetime'].dt.strftime('%y-%m-%d-%H')
    return df


def hourly_pipe(tweets_hourly, btc_hourly):
    """
    Preprocessing steps for hourly analysis. Merges preprocessed BTC dataframe with preprocessed tweets dataframe,
    normalizes some measurements.
    :param tweets_hourly: pd.DataFrame, tweets dataframe
    :param btc_hourly: pd.DataFrame, btc dataframe
    :return: pd.DataFrame
    """
    tweets_hourly_df = tweets_hourly.copy(deep=True)
    btc_hourly_df = btc_hourly.copy(deep=True)
    btc_hourly_df = btc_hourly_pipe(btc_hourly_df)
    tweets_hourly_df = tweet_hourly_pipe(tweets_hourly_df)
    dataframe = pd.merge(btc_hourly_df, tweets_hourly_df, left_on='date',
                         right_on='tweet_created', how='right')
    dataframe.set_index('input_datetime', inplace=True)
    dataframe.dropna(how='any', axis=0, inplace=True)  # drops the last hour for which tweets exist but BTC stats don't
    dataframe[['high_price', 'low_price', 'open_price', 'close_price', 'volumeto', 'volumefrom']] \
        = dataframe[['high_price', 'low_price', 'open_price', 'close_price', 'volumeto', 'volumefrom']].astype(int)
    scaler = MinMaxScaler()
    dataframe[['high_price_norm', 'low_price_norm', 'open_price_norm', 'close_price_norm', 'volumeto_norm',
               'volumefrom_norm', 'avg_vader_compound_norm', 'avg_tb_polarity_norm', 'avg_tb_subjectivity_norm',
               'tweet_count_norm']] \
        = scaler.fit_transform(dataframe[['high_price', 'low_price', 'open_price', 'close_price', 'volumeto',
                                          'volumefrom', 'avg_vader_compound', 'avg_tb_polarity', 'avg_tb_subjectivity',
                                          'tweet_count']])
    return dataframe


dashboard_pipe = Pipeline(steps=[
    ('to_datetime', FunctionTransformer(func=to_datetime, kw_args={'columns': ['tweet_created']}))
])


def get_tweets():
    """
    Retrieves analyzed tweets with their date, author and conversation_ids from 'raw_tweets_info',
    'preprocessed_tweets_info' databases.
    :return: pd.DatFrame
    """
    return pd.DataFrame(retrieve_data(query=query_tweets),
                        columns=['tweet_created', 'vader_compound', 'tb_polarity',
                                 'tb_subjectivity', 'author_id', 'conversation_id'])


def get_daily_btc():
    """
    Retrieves Daily Bitcoin information from the 'btc_daily_info' database.
    :return: pd.DataFrame
    """
    return pd.DataFrame(retrieve_data(query_btc_daily),
                        columns=['input_date', 'open_price', 'high_price', 'low_price', 'close_price',
                                 'adj_close_price', 'volume'])


def get_hourly_btc():
    """
    Retrieves hourly Bitcoin information from the 'btc_hourly_info' database.
    :return: pd.DataFrame
    """
    return pd.DataFrame(retrieve_data(query_btc_hourly),
                        columns=['input_datetime', 'high_price', 'low_price', 'open_price', 'close_price', 'volumeto',
                                 'volumefrom'])


def query_df(dataframe: pd.DataFrame, author_ids: list):
    """
    Used for optimization. Instead of querying database for selected sources, it queries serialized database for the
    selected sources.
    :param dataframe: pd.DataFrame, to be queried
    :param author_ids: list, of Author IDs that are selected
    :return: pd.DataFrame, of all the related conversations to the selected Authors' IDs
    """
    valid_conversations = dataframe.loc[dataframe['author_id'].isin(author_ids)]['conversation_id']
    return dataframe.loc[dataframe['conversation_id'].isin(valid_conversations)]


def get_dataframes():
    """
    Retrieves data from databases and returns dataframes.
    :return: tuple of pd.DataFrames, of preprocessed tweets, btc_daily, btc_hourly
    """
    return get_tweets(), get_daily_btc(), get_hourly_btc()


def parse_input(dataframe: pd.DataFrame, indices: list):
    """
    Parses callbacks to locate account names and account IDs that were selected. Returned values are mainly used to
    filter serialized db.
    Params:
    :param dataframe: pd.DataFrame, acc_corr_table
    :param indices: list, selected indices from acc_corr_table
    :return: tuple, Account IDs and Account Names of the selected sources
    """
    acc_names = dataframe.iloc[indices, 1].values.tolist()  # 1 Column is Account_Name column
    acc_ids = [GET_ID[name] for name in acc_names]
    return acc_ids, acc_names


def round_dash_table(dataframe: pd.DataFrame):
    """
    Rounded dataframe after being converted to_dict('records') unrounds itself for some reason. Function loops over
    converted dataframe and rounds it manually.
    Params:
    :param dataframe: pd.DataFrame, dataframe to be rounded
    :return: pd.DataFrame.to_dict('records')
    """
    records = list()
    columns_to_round = ['BTC Close', 'BTC Open', 'BTC High', 'BTC Low', 'Avg Count']
    for row in dataframe.to_dict('records'):
        for column in columns_to_round:
            row[column] = round(row[column], 2)
        records.append(row)
    return records


def acc_corr_table(corr_with, data_tweets, btc_df, daily=True):
    """
    Creates correlation table for each source vs BTC measurements
    :param corr_with: str, comparison value ['avg_vader_compound', 'avg_tb_subjectivity', 'avg_tb_polarity']
    :param data_tweets: pd.DatFrame, preprocessed data tweets table
    :param btc_df: pd.DataFrame, btc table with either daily or hourly freq
    :param daily: bool, if True creates table for daily analysis, else hourly
    :return: pd.DataFrame
    """
    corr_dict = {
        'Account': [],
        'Account_Name': [],
        'BTC Close': [],
        'BTC Open': [],
        'BTC High': [],
        'BTC Low': [],
        'Count': [],
        'Avg Count': [],
    }
    cols = ['close_price', 'open_price', 'high_price', 'low_price']
    for id_, label in ACCOUNTS.items():
        filtered_tweets = query_df(data_tweets, [id_])
        if daily:
            df = daily_pipe(filtered_tweets, btc_df)
        else:
            df = hourly_pipe(filtered_tweets, btc_df)
        corr = df[cols].corrwith(df[corr_with], ).to_dict()
        corr_dict['Account'].append(f'[{label}](https://twitter.com/{label})')
        corr_dict['Account_Name'].append(f'{label}')
        corr_dict['BTC Close'].append(corr['close_price'])
        corr_dict['BTC Open'].append(corr['high_price'])
        corr_dict['BTC High'].append(corr['open_price'])
        corr_dict['BTC Low'].append(corr['low_price'])
        corr_dict['Count'].append(df['tweet_count'].sum())
        corr_dict['Avg Count'].append(df['tweet_count'].sum() / df.__len__())
    return pd.DataFrame(corr_dict).sort_values(by='BTC Close', ascending=False).reset_index(drop=True)
