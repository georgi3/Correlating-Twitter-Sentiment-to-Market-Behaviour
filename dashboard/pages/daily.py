import numpy as np
import pandas as pd
import json
import dash
import time
from dash import html, dcc, Output, Input, callback
import dash_bootstrap_components as dbc
from data_managing.db_handler import retrieve_data
from dashboard.other.supporting_scripts import query_df, parse_input, plot_sentiment_btc_timeseries, \
    plot_volume_tweet_count_timeseries, plot_correlation_heatmap, plot_pie_chart, create_dash_table, acc_corr_table
from dashboard.other.serialization import read_serialized
from dashboard.other.settings import ACCOUNTS, TS_DROPDOWN_OPTIONS_X, TS_DROPDOWN_OPTIONS_Y, HM_DROPDOWN_OPTIONS, \
    DROPDOWN_SENTIMENT

from src.archive.data_managing.preprocessing import daily_pipe

dash.register_page(__name__, path='/daily-analysis/')


@callback(
    Output('corr-table-daily', 'children'),
    Input('corr-table-storage', 'data'),
    Input('filtered_data_storage', 'data'),
)
def datatable(session_storage, stored_data):
    if stored_data is None:
        indices = [0]
    else:
        indices = json.loads(stored_data)['selected_indices']
    df = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    return create_dash_table(df, indices)


@callback(
    Output('pie-chart-daily', 'figure'),
    Input('datatable', 'selected_rows'),
    Input('corr-table-storage', 'data'),
)
def pie_chart(indices, session_storage):
    dataframe = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    _, checked_sources = parse_input(dataframe, indices)
    return plot_pie_chart(dataframe, checked_sources)


@callback(
    Output('ts-sentiment-btc-daily', 'figure'),
    Input('ts-dropdown-y-daily', 'value'),
    Input('ts-dropdown-y-daily', 'options'),
    Input('ts-dropdown-x-daily', 'value'),
    Input('ts-dropdown-x-daily', 'options'),
    Input('rolling-window-daily', 'value'),
    Input('filtered_data_storage', 'data'),
)
def sentiment_btc_timeseries(y_value_1, y_label_1, y_value_2, y_label_2, window, filtered_data):
    label_1, label_2 = y_label_1[y_value_1], y_label_2[y_value_2]
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_sentiment_btc_timeseries(data, y_value_1, label_1, y_value_2, label_2, window)


@callback(
    Output('ts-volume-daily', 'figure'),
    Input('filtered_data_storage', 'data'),
)
def volume_vs_tweet_count(filtered_data):
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_volume_tweet_count_timeseries(data)


@callback(
    Output('heatmap-daily', 'figure'),
    Input('hm-dropdown', 'value'),
    Input('filtered_data_storage', 'data'),
)
def heatmap(values, filtered_data):
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_correlation_heatmap(data, values)


corr_with_values = [
    'avg_vader_compound'
    'avg_tb_subjectivity'
    'avg_tb_polarity'
]


# TODO Data Storing Related Functions
@callback(
    Output('corr-table-storage', 'data'),
    Output('corr-table-daily', 'className'),  # ClassName! (no update) https://github.com/plotly/dash/issues/1541
    Input('sentiment_table_daily', 'value'),
)
def store_corr_with(corr_with):
    dataframe = read_serialized(corr_with)
    data = {
        'dataframe': dataframe.to_json(date_format='iso', orient='split'),
    }
    return json.dumps(data), dash.no_update


@callback(
    Output('filtered_data_storage', 'data'),
    Input('datatable', 'selected_rows'),
    Input('corr-table-storage', 'data'),
)
def store_df_for_selected_sources(indices, session_storage):
    # indices_mapping = read_serialized('indices_mapping')  # !!!!!!!!!!
    print(indices)

    data_tweets, btc_daily = read_serialized('data_tweets'), read_serialized('btc_daily')
    loaded_storage = json.loads(session_storage)
    dataframe = pd.read_json(loaded_storage['dataframe'], orient='split')
    accounts_ids, _ = parse_input(dataframe, indices)
    filtered_tweets = query_df(data_tweets, accounts_ids)
    data = daily_pipe(filtered_tweets, btc_daily)
    data_to_store = {
        'filtered_df': data.to_json(date_format='iso', orient='split'),
        'selected_indices': indices,
    }
    return json.dumps(data_to_store)


def build_tabs():
    return html.Div(
        id='tabs',
        className='tabs',
        children=[
            dcc.Tabs(
                id='app-tabs',
                value='tab1',
                className='custom-tbas',
                children=[
                    dcc.Tab(
                        id='sources-tab',
                        label='Comparison Settings',
                        value='tab1',
                        className='custom-tab',
                        selected_className='custom-tab--selected'
                    ),
                    dcc.Tab(
                        id='Charts-tab',
                        label='Comparison Charts',
                        value='tab2',
                        className='custom-tab',
                        selected_className='custom-tab--selected',
                    )
                ]
            )
        ]
    )


def render_tab_1():
    return html.Div(
        children=[
            dcc.Dropdown(
                options=DROPDOWN_SENTIMENT,
                placeholder='Average VADER Compound per Day',
                id="sentiment_table_daily",
                value='avg_vader_compound',
                clearable=False,
            ),
            dcc.Loading(
                id='loading-corr-table-daily',
                type='circle',
                fullscreen=True,
                children=[
                    html.Div(id='corr-table-daily'),
                ]
            ),
            dcc.Graph(id='pie-chart-daily'),
        ],
        style={'width': '48%', 'display': 'inline-block'},
    )


def render_tab_2():
    return html.Div(
        children=[
            html.Div(
                id='ts-dropdown-x-d',
                children=[
                    # html.P("Select:"),
                    dcc.Dropdown(
                        options=TS_DROPDOWN_OPTIONS_X,
                        placeholder='Sentiment Analysis',
                        id="ts-dropdown-x-daily",
                        value='avg_vader_compound_norm',
                        clearable=False,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            html.Div(
                id='ts-dropdown-y-d',
                children=[
                    # html.P("Select:"),
                    dcc.Dropdown(
                        options=TS_DROPDOWN_OPTIONS_Y,
                        placeholder='Bitcoin Price',
                        id="ts-dropdown-y-daily",
                        value='close_price_norm',
                        clearable=False,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            html.Div(
                children=[
                    html.Label('Rolling Mean K'),
                    dcc.Input(
                        id='rolling-window-daily',
                        type='number',
                        value=3,
                        min=2,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'},
            ),
            dcc.Graph(id="ts-sentiment-btc-daily"),
            dcc.Graph(id='ts-volume-daily'),
            html.Div(
                id='hm-dropdown-d',
                children=[
                    dcc.Dropdown(
                        id='hm-dropdown',
                        options=HM_DROPDOWN_OPTIONS,
                        value=['close_price'],
                        multi=True
                    )
                ]
            ),
            dcc.Graph(id="heatmap-daily"),
        ]
    )


@callback(
    Output('tab-content', 'children'),
    Input('app-tabs', 'value'),
)
def render_tab_content(tab_switch):
    if tab_switch == 'tab1':
        return render_tab_1()
    else:
        return render_tab_2()


layout = html.Div(
    id='dash-container',
    children=[
        build_tabs(),
        html.Div(
            id='tab-content'
        ),
        dcc.Store(id='corr-table-storage'),
        dcc.Store(id='filtered_data_storage'),
    ]
)


# def return_init_layout():
#     return html.Div([
#         html.Div(children=[
#             html.Div(
#                 id='datatable-div',
#                 children=[
#                     dcc.Dropdown(
#                         options=DROPDOWN_SENTIMENT,
#                         placeholder='Average VADER Compound per Day',
#                         id="sentiment_table_daily",
#                         value='avg_vader_compound_norm',
#                         clearable=False,
#                     ),
#                     dcc.Loading(
#                         id='loading-corr-table-daily',
#                         type='circle',
#                         fullscreen=True,
#                         children=[
#                             html.Div(id='corr-table-daily'),
#                         ]
#                     ),
#                 ],
#                 style={'width': '48%', 'display': 'inline-block'},
#             ),
#             dcc.Graph(id='pie-chart-daily'),
#             ##        # COMPARISON CHARTS
#             html.Div(
#                 id='ts-dropdown-x-d',
#                 children=[
#                     html.P("Select:"),
#                     dcc.Dropdown(
#                         options=TS_DROPDOWN_OPTIONS_X,
#                         placeholder='Sentiment Analysis',
#                         id="ts-dropdown-x-daily",
#                         value='avg_vader_compound_norm',
#                         clearable=False,
#                     ),
#                 ],
#                 style={'width': '48%', 'display': 'inline-block'}
#             ),
#             html.Div(
#                 id='ts-dropdown-y-d',
#                 children=[
#                     html.P("Select:"),
#                     dcc.Dropdown(
#                         options=TS_DROPDOWN_OPTIONS_Y,
#                         placeholder='Bitcoin Price',
#                         id="ts-dropdown-y-daily",
#                         value='close_price_norm',
#                         clearable=False,
#                     ),
#                 ],
#                 style={'width': '48%', 'display': 'inline-block'}
#             ),
#             html.Div(
#                 children=[
#                     html.Label('Rolling Mean K'),
#                     dcc.Input(
#                         id='rolling-window-daily',
#                         type='number',
#                         value=3,
#                         min=2,
#                     ),
#                 ],
#                 style={'width': '48%', 'display': 'inline-block'},
#             ),
#             dcc.Graph(id="ts-sentiment-btc-daily"),
#             dcc.Graph(id='ts-volume-daily'),
#             html.Div(
#                 id='hm-dropdown-d',
#                 children=[
#                     dcc.Dropdown(
#                         id='hm-dropdown',
#                         options=HM_DROPDOWN_OPTIONS,
#                         value=['close_price'],
#                         multi=True
#                     )
#                 ]
#             ),
#             dcc.Graph(id="heatmap-daily"),
#             dcc.Store(id='corr-table-storage'),
#             dcc.Store(id='filtered_data_storage')
#         ]
#         ),
#     ],
#         style={'display': 'flex', 'flex-direction': 'row'},
#     )

# layout = return_init_layout()
