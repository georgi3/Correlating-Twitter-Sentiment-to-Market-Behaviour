from dash import html
import pandas as pd
import plotly.graph_objects as go
from dash import dash_table
import plotly.express as px
from dashboard.other.settings import ACCOUNTS, df_clean_tweets, query_btc_daily, GET_ID
from data_managing.db_handler import retrieve_data
from src.archive.data_managing.preprocessing import daily_pipe


# def acc_corr_table(corr_with='avg_vader_compound'):
#     global data_tweets, btc_daily
#     corr_dict = {
#         'account': [],
#         'close_price_corr': [],
#         'open_price_corr': [],
#         'high_price_corr': [],
#         'low_price_corr': [],
#     }
#     cols = ['close_price', 'open_price', 'high_price', 'low_price']
#     for acc in ACCOUNTS:
#         filtered_tweets = query_df(data_tweets, [acc['value']])
#         df = daily_pipe(filtered_tweets, btc_daily)
#         corr = df[cols].corrwith(df[corr_with]).apply(lambda x: round(x, 3)).to_dict()
#         corr_dict['account'].append(acc['label'])
#         corr_dict['close_price_corr'].append(corr['close_price'])
#         corr_dict['high_price_corr'].append(corr['high_price'])
#         corr_dict['open_price_corr'].append(corr['open_price'])
#         corr_dict['low_price_corr'].append(corr['low_price'])
#     return pd.DataFrame(corr_dict).sort_values(by='close_price_corr', ascending=False)


# def generate_html_table(dataframe: pd.DataFrame):
#     table = html.Table([
#         html.Thead(
#             html.Tr([html.Th(col) for col in dataframe.columns])
#         ),
#         html.Tbody([
#             html.Tr([
#                 html.Td(dataframe.iloc[i][col]) if col != 'Account'
#                 else html.Td(html.A(dataframe.iloc[i][col],
#                                     href=f'https://twitter.com/{dataframe.iloc[i][col]}',
#                                     target='_blank'))
#                 for col in dataframe.columns
#             ]) for i in range(len(dataframe))
#         ])
#     ],
#         id='corr-table'
#     )
#     return table


def get_tweets(query):
    return pd.DataFrame(retrieve_data(query=query),
                        columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity', 'tb_subjectivity'])


def get_daily_btc(query):
    return pd.DataFrame(retrieve_data(query),
                        columns=['input_date', 'open_price', 'high_price', 'low_price', 'close_price',
                                 'adj_close_price', 'volume'])


def query_df(dataframe: pd.DataFrame, sources):
    valid_conversations = dataframe.loc[dataframe['author_id'].isin(sources)]['conversation_id']
    return dataframe.loc[dataframe['conversation_id'].isin(valid_conversations)]


def get_dataframe():
    tweets_df = pd.DataFrame(retrieve_data(query=df_clean_tweets),
                             columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity',
                                      'tb_subjectivity', 'author_id', 'conversation_id'])
    btc_daily = get_daily_btc(query_btc_daily)
    return tweets_df, btc_daily


def parse_input(dataframe, indices):
    acc_names = dataframe.iloc[indices, 1]
    acc_ids = [GET_ID[name] for name in acc_names]
    return acc_ids, acc_names


def plot_sentiment_btc_timeseries(data, y_value_1, label_1, y_value_2, label_2, window):
    fig = go.Figure()
    fig.add_traces([
        go.Scatter(x=data.index, y=data[y_value_1], name=f'{label_1}',
                   mode='lines', line={'dash': 'solid', 'color': 'blue'}),
        go.Scatter(x=data.index, y=data[y_value_1].rolling(window).mean(), name=f'{label_1};'
                                                                                f'<br> Rolling Window {window}',
                   mode='lines', line={'dash': 'dash', 'color': 'blue'}),
        go.Scatter(x=data.index, y=data[y_value_2], name=f'{label_2}',
                   mode='lines', line={'dash': 'solid', 'color': 'red'}),
        go.Scatter(x=data.index, y=data[y_value_2].rolling(window).mean(), name=f'{label_2};'
                                                                                f'<br> Rolling Window {window}',
                   mode='lines', line={'dash': 'dash', 'color': 'red'}),
    ])
    corr_reg = pd.concat([data[y_value_1], data[y_value_2]], axis=1).corr().iloc[1, 0]
    corr_rol = pd.concat([data[y_value_1].rolling(window).mean(), data[y_value_2].rolling(window).mean()],
                         axis=1).corr().iloc[1, 0]
    text = f'Solid Lines Corr: <b>{corr_reg: .3f}</b><br>' \
           f'Dashed Lines Corr: <b>{corr_rol: .3f}</b>'
    fig_layout = go.Layout(
        xaxis=go.layout.XAxis(domain=[0, 1]),
        title=f'Timeseries for {label_1} and {label_2}',
        annotations=[
            go.layout.Annotation(
                showarrow=False,
                align='left',
                yanchor='bottom',
                text=text,
                xref='paper',
                yref='paper',
                x=1.3,
                y=0.2
            ),
        ]
    )
    fig.layout = fig_layout
    return fig


def plot_volume_tweet_count_timeseries(data, sources):
    fig = go.Figure()
    fig.add_traces([
        go.Bar(x=data.index, y=data['tweet_count_norm'], name='Tweet Count Normalized'),
        go.Scatter(x=data.index, y=data['tweet_count_norm'], name='Tweet Count Normalized', mode='lines',
                   line={'dash': 'dash', 'color': 'blue'}),
        go.Scatter(x=data.index, y=data['volume_norm'], name='BTC Volume Normalized', mode='lines',
                   line={'dash': 'solid', 'color': 'black'}),
        go.Scatter(x=data.index, y=data['high_price_norm'], name='BTC High Normalized', mode='lines',
                   line={'dash': 'solid', 'color': 'red'})
    ])
    fig.update_layout({'title': f'Timeseries for Normalized BTC Volume and Tweet Count from {tuple(sources)}'})
    return fig


def plot_correlation_heatmap(data, values):
    columns = ['avg_vader_compound', 'avg_tb_polarity', 'avg_tb_subjectivity', 'tweet_count', 'volume']
    columns.extend(values)
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr, title='Correlations')
    return fig


def plot_pie_chart(data, sources):
    fig = px.pie(data_frame=data.loc[data['Account_Name'].isin(sources)],
                 names='Account_Name', values='Count', color='Account_Name')
    return fig


def create_dash_table(dataframe: pd.DataFrame):
    df = dataframe[['Account', 'BTC Close', 'BTC Open', 'BTC High', 'BTC Low', 'Count']]
    return dash_table.DataTable(
        id='datatable',
        columns=[
            {'name': i, 'id': i, 'presentation': 'markdown'} if i == 'Account'
            else {'name': i, 'id': i} for i in df.columns
        ],
        data=df.to_dict('records'),
        filter_action='native',
        sort_action='native',
        sort_mode='multi',
        row_selectable='multi',
        selected_rows=[0],
        # selected_row_ids=[],  # https://github.com/plotly/dash/issues/185 BUG (returns [None, None, ...])
        page_action='native',
        tooltip={i: {'value': i, 'use_with': 'header'} for i in df.columns},
        markdown_options={'html': False, 'link_target': '_blank'},
        # page_current=0,
        # page_count=11,
    )
