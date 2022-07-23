import pandas as pd
import dash
from dash import Dash, html, dcc, Output, Input, callback
import plotly.graph_objects as go
import plotly.express as px
from src.archive.data_managing.preprocessing import hourly_pipe
from data_managing.db_handler import retrieve_data

# CONF
ACCOUNTS = [
    {'label': 'cz_binance', 'value': '902926941413453824'},
    {'label': 'TheCryptoLark', 'value': '30325257'},
    {'label': 'Sheldon_Sniper', 'value': '1374629644884971521'},
    {'label': 'DocumentingBTC', 'value': '1337780902680809474'},
    {'label': 'BitcoinMagazine', 'value': '361289499'},
    {'label': 'BitcoinFear', 'value': '1151046460688887808'},
    {'label': 'BTC_Archive', 'value': '970994516357472257'},
    {'label': 'Bitcoin', 'value': '357312062'},
    {'label': 'BT', 'value': '432093'},
    {'label': 'MartiniGuyYT', 'value': '782946231551131648'},
    {'label': 'binance', 'value': '877807935493033984'},
    {'label': 'CoinDesk', 'value': '1333467482'},
]
DEFAULT = ['361289499', '1333467482']

df_clean_tweets = f"""
SELECT tweet_created, cleaned_text, vader_compound, text_blob_polarity, text_blob_subjectivity, author_id,
        conversation_id
FROM preprocessed_tweets_info 
LEFT OUTER JOIN raw_tweets_info rti ON preprocessed_tweets_info.tweet_id = rti.tweet_id
"""
query_btc_hourly = """
SELECT input_datetime, high_price, low_price, open_price, close_price, volumeto, volumefrom FROM btc_hourly_info
"""

dash.register_page(__name__, path='/hourly-analysis/')

layout = html.Div([
    html.Div(children=[
        html.H1('Daily Analysis'),
        html.Label('Twitter Sources'),
        dcc.Checklist(
            id='sources_checklist',
            options=ACCOUNTS,
            value=['902926941413453824'],
            inline=False
        ),
        dcc.Graph(id="timeseries"),
        dcc.Graph(id="heatmap"),
        dcc.Graph(id="bar_chart"),

        html.Br(),
        html.Br(),
        # ], style={'padding': 10, 'flex': 1},

        # html.Div([
        #     html.H4('Interactive color selection with simple Dash example'),
        #     html.P("Select color:"),
        #     dcc.Dropdown(
        #         id="dropdown",
        #         options=['Gold', 'MediumTurquoise', 'LightGreen'],
        #         value='Gold',
        #         clearable=False,
        #     ),
        #     dcc.Graph(id="graph"),
        # ])
    ]
    )
],
    style={'display': 'flex', 'flex-direction': 'row'},
)


@callback(
    Output('timeseries', 'figure'),
    Input('sources_checklist', 'value')
)
def plot_time_series(sources):
    global data_tweets, btc_hourly
    filtered_tweets = query_df(data_tweets, sources)
    data = hourly_pipe(filtered_tweets, btc_hourly)
    y = ['close_norm', 'avg_vader_compound_norm', 'avg_tb_subjectivity_norm', 'avg_tb_polarity_norm']
    fig = px.line(data, x=data.index, y=y)
    return fig


@callback(
    Output('heatmap', 'figure'),
    Input('sources_checklist', 'value')
)
def plot_heat_map(sources):
    global data_tweets, btc_hourly
    filtered_tweets = query_df(data_tweets, sources)
    data = hourly_pipe(filtered_tweets, btc_hourly)
    columns = ['close_price', 'open_price', 'high_price', 'low_price',
               'avg_vader_compound', 'avg_tb_subjectivity', 'avg_tb_polarity']
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr)
    return fig


@callback(
    Output('bar_chart', 'figure'),
    Input('sources_checklist', 'value')
)
def plot_tweet_count(sources):
    global data_tweets, btc_hourly
    filtered_tweets = query_df(data_tweets, sources)
    data = hourly_pipe(filtered_tweets, btc_hourly)
    fig = px.bar(data, x=data.index, y='tweet_count')
    return fig


def get_tweets(query):
    return pd.DataFrame(retrieve_data(query=query),
                        columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity', 'tb_subjectivity'])


def get_hourly_btc(query):
    return pd.DataFrame(retrieve_data(query),
                        columns=['input_datetime', 'high_price', 'low_price', 'open_price', 'close_price', 'volumeto',
                                 'volumefrom'])


def query_df(dataframe: pd.DataFrame, sources: list):
    valid_conversations = dataframe.loc[dataframe['author_id'].isin(sources)]['conversation_id']
    return dataframe.loc[dataframe['conversation_id'].isin(valid_conversations)]


def get_dataframe():
    tweets_df = pd.DataFrame(retrieve_data(query=df_clean_tweets),
                             columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity',
                                      'tb_subjectivity', 'author_id', 'conversation_id'])
    btc_daily = get_hourly_btc(query_btc_hourly)
    return tweets_df, btc_daily


# data_tweets = pd.read_csv('tweets.csv')
# btc_hourly = pd.read_csv('hourly_btc.csv')
data_tweets, btc_hourly = get_dataframe()
