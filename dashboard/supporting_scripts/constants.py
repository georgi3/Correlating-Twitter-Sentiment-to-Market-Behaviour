DROPDOWN_SENTIMENT = {
    'avg_vader_compound': 'Average VADER Compound per Day',
    'avg_tb_subjectivity': 'Average TextBlob subjectivity per Day',
    'avg_tb_polarity': 'Average TextBlob polarity per Day',
}
ACCOUNTS = {
    '902926941413453824': 'cz_binance',
    '30325257': 'TheCryptoLark',
    '1374629644884971521': 'Sheldon_Sniper',
    '1337780902680809474': 'DocumentingBTC',
    '361289499': 'BitcoinMagazine',
    '1151046460688887808': 'BitcoinFear',
    '970994516357472257': 'BTC_Archive',
    '357312062': 'Bitcoin',
    '432093': 'BT',
    '782946231551131648': 'MartiniGuyYT',
    '877807935493033984': 'binance',
    '1333467482': 'CoinDesk',
}
GET_ID = {
    'cz_binance': '902926941413453824',
    'TheCryptoLark': '30325257',
    'Sheldon_Sniper': '1374629644884971521',
    'DocumentingBTC': '1337780902680809474',
    'BitcoinMagazine': '361289499',
    'BitcoinFear': '1151046460688887808',
    'BTC_Archive': '970994516357472257',
    'Bitcoin': '357312062',
    'BT': '432093',
    'MartiniGuyYT': '782946231551131648',
    'binance': '877807935493033984',
    'CoinDesk': '1333467482',
}

TOOLTIP_INFO_D = [
    'Twitter Account',
    "Account's correlation against Bitcoin Close Price",
    "Account's correlation against Bitcoin Open Price",
    "Account's correlation against Bitcoin High Price",
    "Account's correlation against Bitcoin Low Price",
    'Total count of tweets&comments analyzed per account.',
    'Average count of tweets&comments per day.'
]

TOOLTIP_INFO_H = [
    "Account's correlation against Twitter Account",
    "Account's correlation against Bitcoin Close Price",
    "Account's correlation against Bitcoin Open Price",
    "Account's correlation against Bitcoin High Price",
    "Account's correlation against Bitcoin Low Price",
    'Total count of tweets&comments analyzed per account.',
    'Average count of tweets&comments per hour.'
]

TS_DROPDOWN_OPTIONS_X = {
    'avg_vader_compound': 'Average VADER Compound per Day',
    'avg_tb_subjectivity': 'Average TextBlob subjectivity per Day',
    'avg_tb_polarity': 'Average TextBlob polarity per Day',
    'avg_vader_compound_norm': 'Average VADER Compound per Day, Normalized',
    'avg_tb_subjectivity_norm': 'Average TextBlob subjectivity per Day, Normalized',
    'avg_tb_polarity_norm': 'Average TextBlob polarity per Day, Normalized'
}

TS_DROPDOWN_OPTIONS_Y = {
    'open_price': 'Bitcoin Open',
    'close_price': 'Bitcoin Close',
    'high_price': 'Bitcoin High',
    'low_price': 'Bitcoin Low',
    'open_price_norm': 'Bitcoin Open Normalized',
    'close_price_norm': 'Bitcoin Close Normalized',
    'high_price_norm': 'Bitcoin High Normalized',
    'low_price_norm': 'Bitcoin Low Normalized',
}
HM_DROPDOWN_OPTIONS = [
    {'label': 'Bitcoin Close Price', 'value': 'close_price'},
    {'label': 'Bitcoin Open Price', 'value': 'open_price'},
    {'label': 'Bitcoin High Price', 'value': 'high_price'},
    {'label': 'Bitcoin Low Price', 'value': 'low_price'},
]
TS_VOLUME_HOURLY = {
    'volumeto': 'Volume To',
    'volumefrom': 'Volume From',
    'volumeto_norm': 'Volume To Normalized',
    'volumefrom_norm': 'Volume From Normalized'
}
CORR_WITH_VALUES = [
    'avg_vader_compound',
    'avg_tb_subjectivity',
    'avg_tb_polarity',
]

query_tweets = f"""
SELECT tweet_created, vader_compound, text_blob_polarity, text_blob_subjectivity, author_id,
        conversation_id
FROM preprocessed_tweets_info 
LEFT OUTER JOIN raw_tweets_info rti ON preprocessed_tweets_info.tweet_id = rti.tweet_id
"""
query_btc_daily = """
SELECT input_date, open_price, high_price, low_price, close_price, adj_close_price, volume FROM btc_daily_info
"""
query_btc_hourly = """
SELECT input_datetime, high_price, low_price, open_price, close_price, volumeto, volumefrom FROM btc_hourly_info
"""