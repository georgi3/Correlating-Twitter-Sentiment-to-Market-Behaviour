from dashboard.supporting_scripts.constants import CORR_WITH_VALUES
from dashboard.supporting_scripts.misc import acc_corr_table, get_dataframes
import pickle
import os


def serialize(**kwargs):
    """
    Helper function, mainly works with task scheduler to serialize heavy database every hour, because that is how
    often database gets updated. Is also used to serialize other objects to improve speed performance.
    :param kwargs: name of the object and object itself
    """
    if not os.path.isdir('./serialized'):
        os.mkdir('./serialization')
    for label, df in kwargs.items():
        with open(f'serialized/{label}.pickle', 'wb') as f:
            pickle.dump(df, f)


def read_serialized(label: str):
    """
    Reads serialized objects
    :param label: str, name of the serialized object
    :return: pd.DataFrame
    """
    try:
        with open(f'serialized/{label}.pickle', 'rb') as f:
            dataframe = pickle.load(f)
    except FileNotFoundError as err:
        raise FileNotFoundError(err)
    return dataframe


def serialize_db():
    """
    Function that is scheduled to run hourly. It serializes database with tweets abd BTC on hourly basis and does some
    computations in advance which are also serialized to speed up site speed.
        Serializes:
            - data_tweets: pd.DataFrame, (note: very large and constantly growing)
            - btc_daily: pd.DataFrame, bitcoin daily stats
            - btc_hourly: pd.DataFrame, bitcoin hourly stats
            - indices_mapping_d: dict, (note: takes some time to calculate that is why it is serialized hourly)
                - avg_vader_compound_d: pd.DataFrame, Correlation/Stats Table for every source vs btc stats
                - avg_tb_subjectivity_d: pd.DataFrame, Correlation/Stats Table for every source vs btc stats
                - avg_tb_polarity_d: pd.DataFrame, Correlation/Stats Table for every source vs btc stats
            - indices_mapping_h: dict, (note: takes some time to calculate that is why it is serialized hourly)
                - avg_vader_compound_h: pd.DataFrame, Correlation/Stats Table for every source vs btc stats
                - avg_tb_subjectivity_h: pd.DataFrame, Correlation/Stats Table for every source vs btc stats
                - avg_tb_polarity_h: pd.DataFrame, Correlation/Stats Table for every source vs btc stats

    """
    data_tweets, btc_daily, btc_hourly = get_dataframes()
    data_to_serialize = {
        'data_tweets': data_tweets,
        'btc_daily': btc_daily,
        'btc_hourly': btc_hourly,
    }
    indices_mapping_d = dict()
    indices_mapping_h = dict()
    for value in CORR_WITH_VALUES:
        corr_table_d = acc_corr_table(value, data_tweets, btc_daily, daily=True)
        data_to_serialize[f'{value}_d'] = corr_table_d
        indices_mapping_d[value] = {
            'names': {name: i for i, name in zip(corr_table_d.index, corr_table_d.Account_Name)}
        }
        corr_table_h = acc_corr_table(value, data_tweets, btc_hourly, daily=False)
        data_to_serialize[f'{value}_h'] = corr_table_h
        indices_mapping_h[value] = {
            'names': {name: i for i, name in zip(corr_table_h.index, corr_table_h.Account_Name)}
        }
    data_to_serialize['indices_mapping_d'] = indices_mapping_d
    data_to_serialize['indices_mapping_h'] = indices_mapping_h
    serialize(**data_to_serialize)



