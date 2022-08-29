import pandas as pd
import json
import dash
from dash import Output, Input, State, callback
from dashboard.supporting_scripts.reusable_html import create_dash_table, \
    plot_sentiment_btc_timeseries, plot_pie_chart, plot_correlation_heatmap, plot_volume_tweet_count_timeseries
from dashboard.supporting_scripts.misc import query_df, parse_input, daily_pipe
from dashboard.supporting_scripts.daily_html import render_tab_1, render_tab_2, layout
from dashboard.supporting_scripts.serialization import read_serialized
from dashboard.supporting_scripts.constants import TOOLTIP_INFO_D

dash.register_page(__name__, path='/daily-analysis/')


@callback(
    Output('tab-content-d', 'children'),
    Input('app-tabs-d', 'value'),
)
def render_tab_content(tab_switch):
    """
    Callback is responsible for displaying tab content when switched.
    :param tab_switch: str, ['tab1', 'tab2']
    :return: html.Div()
    """
    if tab_switch == 'tab1':
        return render_tab_1()
    else:
        return render_tab_2()


# Data Storing Related Functions
@callback(
    Output('corr-table-storage-d', 'data'),
    Output('corr-table-div-d', 'className'),  # dummy output just to trigger the loading state on 'corr-table-daily'
    # https://github.com/plotly/dash/issues/1541
    Input('sentiment-table-d-dp', 'value'),
)
def store_corr_with(corr_with):
    """
    Callback is responsible for storing corr_table dataframe which is already serialized.
    :param corr_with: str, ['avg_tb_polarity', 'avg_tb_subjectivity', 'avg_vader_compound']
    :return: JSON (pd.DataFrame)
    """
    dataframe = read_serialized(f'{corr_with}_d')
    data = {
        'dataframe': dataframe.to_json(date_format='iso', orient='split'),
    }
    return json.dumps(data), dash.no_update


@callback(
    Output('filtered-data-storage-d', 'data'),
    Input('corr-table-d', 'selected_rows'),
    State('corr-table-storage-d', 'data'),
)
def store_df_for_selected_sources(indices, session_storage):
    """
    Callback is responsible for storing filtered dataframe (from selected sources), and account names of selected
     sources.
    :param indices: list, selected indices from corr-table
    :param session_storage: JSON (pd.DataFrame), corr_table_storage
    :return: JSON (pd.DataFrame, list)
    """
    if not indices:
        indices = [0]
    data_tweets, btc_daily = read_serialized('data_tweets'), read_serialized('btc_daily')
    loaded_storage = json.loads(session_storage)
    dataframe = pd.read_json(loaded_storage['dataframe'], orient='split')
    accounts_ids, acc_names = parse_input(dataframe, indices)
    filtered_tweets = query_df(data_tweets, accounts_ids)
    data = daily_pipe(filtered_tweets, btc_daily)
    data_to_store = {
        'filtered_df': data.to_json(date_format='iso', orient='split'),
        'selected_sources': acc_names,
    }
    return json.dumps(data_to_store)


@callback(
    Output('corr-table-div-d', 'children'),
    Input('corr-table-storage-d', 'data'),
    Input('close_error-d', 'n_clicks'),
    State('not_selected_sources_error-d', 'is_open'),
    State('filtered-data-storage-d', 'data'),
    State('sentiment-table-d-dp', 'value'),
)
def datatable(session_storage, error, modal_state, stored_data, corr_with):
    """
    Callback is responsible for displaying corr-table with sources
    :param session_storage: JSON (pd.DataFrame), corr_table
    :param error: int, arg is not used, works only as a callback trigger
    :param modal_state: bool, whether error modal (sources not selected) is open or closed
    :param stored_data: JSON (pd.DataFrame, list) only list with account names of selected sources is used
    :param corr_with: str, ['avg_tb_polarity', 'avg_tb_subjectivity', 'avg_vader_compound']
     to what sentiment BTC is correlated to
    :return: dash_table.DataTable()
    """
    element_id = 'corr-table-d'
    df = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    if modal_state:
        return create_dash_table(df, [0], element_id, TOOLTIP_INFO_D)
    if stored_data is None:
        indices = [i for i in range(12)]
    else:
        sources = json.loads(stored_data)['selected_sources']
        indices_mapping = read_serialized('indices_mapping_d')
        indices = [indices_mapping[corr_with]['names'][source] for source in sources]
    return create_dash_table(df, indices, element_id, TOOLTIP_INFO_D)


# Charts
@callback(
    Output('pie-chart-d', 'figure'),
    Input('corr-table-d', 'selected_rows'),
    State('corr-table-storage-d', 'data'),
)
def pie_chart(indices, session_storage):
    """
    Callback is responsible for displaying pie chart aka distribution of tweets by source between selected sources
    :param indices: list, selected sources
    :param session_storage: JSON (pd.DataFrame), of corr-table
    :return: figure
    """
    dataframe = pd.read_json(json.loads(session_storage)['dataframe'], orient='split')
    _, checked_sources = parse_input(dataframe, indices)
    return plot_pie_chart(dataframe, checked_sources)


@callback(
    Output('ts-sentiment-btc-d', 'figure'),
    Input('ts-dropdown-y-d', 'value'),
    Input('ts-dropdown-x-d', 'value'),
    Input('rolling-window-d', 'value'),
    Input('filtered-data-storage-d', 'data'),
    State('ts-dropdown-y-d', 'options'),
    State('ts-dropdown-x-d', 'options')
)
def sentiment_btc_timeseries(btc_price_kind, sentiment_kind, window, filtered_data, btc_labels, sentiment_labels):
    """
    Callback responsible for displaying timeseries chart for btc, sentiment, rolling window
    :param btc_price_kind: str, ['open_price', 'open_price_norm', 'high_price', 'high_price_norm', 'low_price',
     'low_price_norm', 'close_price', 'close_price_norm']
    :param sentiment_kind: str, ['avg_tb_polarity', 'avg_tb_subjectivity', 'avg_vader_compound', 'avg_tb_polarity_norm',
     'avg_tb_subjectivity_norm', 'avg_vader_compound_norm']
    :param window: int, >=2 rolling window size
    :param filtered_data: JSON (pd.DataFrame, list), only dataframe is used to construct timeseries
    :param btc_labels: str, label to display for btc_price_kind
    :param sentiment_labels: str, label to display for low_price_norm
    :return: figure
    """
    label_1, label_2 = btc_labels[btc_price_kind], sentiment_labels[sentiment_kind]
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_sentiment_btc_timeseries(data, btc_price_kind, label_1, sentiment_kind, label_2, window)


@callback(
    Output('ts-volume-d', 'figure'),
    Input('filtered-data-storage-d', 'data'),
)
def volume_vs_tweet_count(filtered_data):
    """
    Callback is responsible for displaying timeseries for BTC volume and tweet volume
    :param filtered_data: JSON (pd.DataFrame, list), only dataframe is used
    :return: figure
    """
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_volume_tweet_count_timeseries(data)


@callback(
    Output('heatmap-d', 'figure'),
    Input('hm-dropdown-d', 'value'),
    Input('filtered-data-storage-d', 'data'),
)
def heatmap(values, filtered_data):
    """
    Callback is responsible for displaying correlation heatmap
    :param values: list, ['open_price', 'close_price', 'high_price', 'low_price']
    :param filtered_data: JSON (pd.DataFrame, list), only dataframe is used
    :return: figure
    """
    columns = ['avg_vader_compound', 'avg_tb_polarity', 'avg_tb_subjectivity', 'tweet_count', 'volume']
    data = pd.read_json(json.loads(filtered_data)['filtered_df'], orient='split')
    return plot_correlation_heatmap(data, values, columns)


# Error handling
@callback(
    Output('not_selected_sources_error-d', 'is_open'),
    Input('corr-table-d', 'selected_rows')
)
def toggle_error_modal(indices):
    """
    Error not selected sources, modal with error message pops up
    :param indices: list, if falsy error is displayed
    :return: dbc.Modal
    """
    if indices:
        return False
    else:
        return True


layout()
