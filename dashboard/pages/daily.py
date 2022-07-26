import pandas as pd
import json
import dash
from dash import html, dcc, Output, Input, callback, dash_table

from data_managing.db_handler import retrieve_data
from src.archive.data_managing.preprocessing import daily_pipe
from dashboard.other.supporting_scripts import query_df, get_dataframe, parse_input, plot_sentiment_btc_timeseries, \
    plot_volume_tweet_count_timeseries, plot_correlation_heatmap, plot_pie_chart, create_dash_table
from dashboard.other.settings import ACCOUNTS, TS_DROPDOWN_OPTIONS_X, TS_DROPDOWN_OPTIONS_Y, HM_DROPDOWN_OPTIONS,\
    DROPDOWN_SENTIMENT

dash.register_page(__name__, path='/daily-analysis/')


@callback(
    Output('datatable_div', 'children'),
    Input('corr_with_value', 'data'),
)
def datatable(session_storage):
    df = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    return create_dash_table(df)


g@callback(
    Output('pie-fig', 'figure'),
    Input('datatable', 'selected_rows'),
    Input('corr_with_value', 'data'),
)
def pie_chart(indices, session_storage):
    dataframe = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    _, checked_sources = parse_input(dataframe, indices)
    return plot_pie_chart(dataframe, checked_sources)


@callback(
    Output('daily-timeseries', 'figure'),
    Input('ts-dropdown-y', 'value'),
    Input('ts-dropdown-y', 'options'),
    Input('ts-dropdown-x', 'value'),
    Input('ts-dropdown-x', 'options'),
    Input('rolling-window', 'value'),
    Input('filtered_data_storage', 'data'),
)
def sentiment_btc_timeseries(y_value_1, y_label_1, y_value_2, y_label_2, window, filtered_data):
    label_1, label_2 = y_label_1[y_value_1], y_label_2[y_value_2]
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_sentiment_btc_timeseries(data, y_value_1, label_1, y_value_2, label_2, window)


@callback(
    Output('daily-volume', 'figure'),
    Input('datatable', 'selected_rows'),
    Input('corr_with_value', 'data'),
    Input('filtered_data_storage', 'data'),
)
def volume_vs_tweet_count(indices, session_storage, filtered_data):
    dataframe = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    accounts_ids, _ = parse_input(dataframe, indices)
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    sources = [ACCOUNTS[source] for source in accounts_ids]
    return plot_volume_tweet_count_timeseries(data, sources)


@callback(
    Output('daily-heatmap', 'figure'),
    Input('hm-dropdown', 'value'),
    Input('filtered_data_storage', 'data'),
)
def heatmap(values, filtered_data):
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_correlation_heatmap(data, values)


@callback(
    Output('corr_with_value', 'data'),
    Input('sentiment_table_dd', 'value')
)
def store_corr_with(corr_with):
    dataframe = acc_corr_table(corr_with)
    data = {
        'corr_with': corr_with,
        'dataframe': dataframe.to_json(date_format='iso', orient='split'),
    }
    return json.dumps(data)


@callback(
    Output('filtered_data_storage', 'data'),
    Input('datatable', 'selected_rows'),
    Input('corr_with_value', 'data')
)
def store_df_for_selected_sources(indices, session_storage):
    dataframe = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    accounts_ids, _ = parse_input(dataframe, indices)
    filtered_tweets = query_df(data_tweets, accounts_ids)
    data = daily_pipe(filtered_tweets, btc_daily)
    data_to_store = {
        'filtered_df': data.to_json(date_format='iso', orient='split')
    }
    return json.dumps(data_to_store)


def acc_corr_table(corr_with):
    global data_tweets, btc_daily
    corr_dict = {
        'Account': [],
        'Account_Name': [],
        'BTC Close': [],
        'BTC Open': [],
        'BTC High': [],
        'BTC Low': [],
        'Count': [],
    }
    cols = ['close_price', 'open_price', 'high_price', 'low_price']
    for id_, label in ACCOUNTS.items():
        filtered_tweets = query_df(data_tweets, [id_])
        df = daily_pipe(filtered_tweets, btc_daily)
        corr = df[cols].corrwith(df[corr_with], ).apply(lambda x: round(x, 3)).to_dict()
        corr_dict['Account'].append(f'[{label}](https://twitter.com/{label})')
        corr_dict['Account_Name'].append(f'{label}')
        corr_dict['BTC Close'].append(corr['close_price'])
        corr_dict['BTC Open'].append(corr['high_price'])
        corr_dict['BTC High'].append(corr['open_price'])
        corr_dict['BTC Low'].append(corr['low_price'])
        corr_dict['Count'].append(df['tweet_count'].sum())
    return pd.DataFrame(corr_dict).sort_values(by='BTC Close', ascending=False)

from dashboard.other.serialization import read_serialized
# data_tweets, btc_daily = get_dataframe()
data_tweets, btc_daily = read_serialized('data_tweets'), read_serialized('btc_daily')

layout = html.Div([
    html.Div(children=[
        html.Div(
            [
                html.P("Select:"),
                dcc.Dropdown(
                    options=DROPDOWN_SENTIMENT,
                    placeholder='Average VADER Compound per Day',
                    id="sentiment_table_dd",
                    value='avg_vader_compound_norm',
                    clearable=False,
                ),
            ],
            style={'width': '48%', 'display': 'inline-block'}),
        html.Div(id='datatable_div'),
        # html.Div(children=[
        #     dash_table.DataTable(
        #         id='datatable',
        #         columns=[
        #             {'name': i, 'id': i, 'presentation': 'markdown'} if i == 'Account'
        #             else {'name': i, 'id': i} for i in dataframe.columns
        #         ],
        #         data=dataframe.to_dict('records'),
        #         # filter_action='native',
        #         sort_action='native',
        #         sort_mode='multi',
        #         row_selectable='multi',
        #         selected_rows=[0],
        #         # selected_row_ids=[],  # https://github.com/plotly/dash/issues/185 BUG (returns [None, None, ...])
        #         page_action='native',
        #         tooltip={i: {'value': i, 'use_with': 'header'} for i in dataframe.columns},
        #         markdown_options={'html': False},
        #         # page_current=0,
        #         # page_count=11,
        #     ),
        #     # html.Div(id='data')
        # ]),
        dcc.Graph(id='pie-fig'),
        html.Div(
            [
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
        html.Div([
            html.Label('Rolling Mean K'),
            dcc.Input(
                id='rolling-window',
                type='number',
                value=3,
                min=2,
            ),
        ],
            style={'width': '48%', 'display': 'inline-block'},
        ),
        dcc.Graph(id="daily-timeseries"),
        dcc.Graph(id='daily-volume'),
        html.Div(children=[
            dcc.Dropdown(
                id='hm-dropdown',
                options=HM_DROPDOWN_OPTIONS,
                value=['close_price'],
                multi=True
            )
        ]),
        dcc.Graph(id="daily-heatmap"),
    ]
    ),
    dcc.Store(id='corr_with_value'),
    dcc.Store(id='filtered_data_storage')
],
    style={'display': 'flex', 'flex-direction': 'row'},
)
