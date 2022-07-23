import pandas as pd
import dash
from dash import html, dcc, Output, Input, callback
import plotly.express as px

from data_managing.db_handler import retrieve_data
from src.archive.data_managing.preprocessing import daily_pipe
from dashboard.other.supporting_scripts import generate_html_table, query_df, get_dataframe
from dashboard.other.settings import ACCOUNTS, TS_DROPDOWN_OPTIONS_X, TS_DROPDOWN_OPTIONS_Y, HM_DROPDOWN_OPTIONS,\
    ACC_MAP

dash.register_page(__name__, path='/daily-analysis/')


@callback(
    Output('daily-timeseries', 'figure'),
    Input('sources_checklist', 'value'),
    Input('ts-dropdown-y', 'value'),
    Input('ts-dropdown-x', 'value')
)
def plot_time_series(sources, y_value_1, y_value_2):
    global data_tweets, btc_daily
    filtered_tweets = query_df(data_tweets, sources)
    data = daily_pipe(filtered_tweets, btc_daily)
    y_values = [y_value_1, y_value_2]
    fig = px.line(data_frame=data, x=data.index, y=y_values,
                  title=f'Timeseries for {y_value_1} and {y_value_2}')
    return fig


@callback(
    Output('daily-volume', 'figure'),
    Input('sources_checklist', 'value'),
)
def plot_volume_vs_tweet_count(sources):
    global data_tweets, btc_daily
    filtered_tweets = query_df(data_tweets, sources)
    data = daily_pipe(filtered_tweets, btc_daily)
    sources = [ACC_MAP[source] for source in sources]
    y_values = ['tweet_count_norm', 'volume_norm']
    fig = px.line(data, x=data.index, y=y_values,
                  title=f'Timeseries for Normalised BTC Volume and Tweet Count from {sources}')
    return fig


@callback(
    Output('daily-heatmap', 'figure'),
    Input('sources_checklist', 'value')
)
def plot_heat_map(sources):
    global data_tweets, btc_daily
    filtered_tweets = query_df(data_tweets, sources)
    data = daily_pipe(filtered_tweets, btc_daily)
    columns = ['close_price', 'open_price', 'high_price', 'low_price',
               'avg_vader_compound', 'avg_tb_subjectivity', 'avg_tb_polarity']  # , 'volume', 'tweet_count']
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr, title='Correlations')
    return fig


def acc_corr_table(corr_with='avg_vader_compound'):
    global data_tweets, btc_daily
    corr_dict = {
        'Account': [],
        'BTC Close Corr': [],
        'BTC Open Corr': [],
        'BTC High Corr': [],
        'BTC Low Corr': [],
        'Tweet&Comment Count': []
    }
    cols = ['close_price', 'open_price', 'high_price', 'low_price']
    for acc in ACCOUNTS:
        filtered_tweets = query_df(data_tweets, [acc['value']])
        df = daily_pipe(filtered_tweets, btc_daily)
        corr = df[cols].corrwith(df[corr_with], ).apply(lambda x: round(x, 3)).to_dict()
        corr_dict['Account'].append(acc['label'])
        corr_dict['BTC Close Corr'].append(corr['close_price'])
        corr_dict['BTC Open Corr'].append(corr['high_price'])
        corr_dict['BTC High Corr'].append(corr['open_price'])
        corr_dict['BTC Low Corr'].append(corr['low_price'])
        corr_dict['Tweet&Comment Count'].append(df['tweet_count'].sum())

    return pd.DataFrame(corr_dict).sort_values(by='BTC Close Corr', ascending=False)


@callback(
    Output('daily-bar_chart', 'figure'),
    Input('sources_checklist', 'value')
)
def plot_tweet_count(sources):
    global data_tweets, btc_daily
    filtered_tweets = query_df(data_tweets, sources)
    data = daily_pipe(filtered_tweets, btc_daily)
    sources = [ACC_MAP[source] for source in sources]
    fig = px.bar(data, x=data.index, y='tweet_count',
                 title=f'Tweet Count per Day from {tuple(sources)}')
    return fig


data_tweets, btc_daily = get_dataframe()

layout = html.Div([
    html.Div(children=[
        html.H4('Table'),
        generate_html_table(acc_corr_table()),
        html.Label('Twitter Sources'),
        dcc.Checklist(
            id='sources_checklist',
            options=ACCOUNTS,
            value=['902926941413453824'],
            inline=False
        ),
        html.Div(
            [
                # html.H4('BTC_Close_Normalised vs -'),
                html.P("Select:"),
                dcc.Dropdown(
                    options=TS_DROPDOWN_OPTIONS_X,
                    placeholder='Sentiment Analysis',
                    id="ts-dropdown-x",
                    value='avg_vader_compound_norm',
                    clearable=False,
                ),
            ],
            style={'width': '48%', 'display': 'inline-block'}),
        html.Div([
            html.P("Select:"),
            dcc.Dropdown(
                options=TS_DROPDOWN_OPTIONS_Y,
                placeholder='Bitcoin Price',
                id="ts-dropdown-y",
                value='close_price_norm',
                clearable=False,
            ),
        ],
            style={'width': '48%', 'display': 'inline-block'}),
        dcc.Graph(id="daily-timeseries"),
        dcc.Graph(id='daily-volume'),
        html.Div(children=[
            dcc.Dropdown(
                options=HM_DROPDOWN_OPTIONS,
                # placeholder=
            )
        ]),
        dcc.Graph(id="daily-heatmap"),
        dcc.Graph(id="daily-bar_chart"),
    ]
    )
],
    style={'display': 'flex', 'flex-direction': 'row'},
)
