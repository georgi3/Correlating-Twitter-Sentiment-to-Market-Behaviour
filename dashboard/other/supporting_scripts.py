from dash import html
import pandas as pd
import plotly.graph_objects as go
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
from dashboard.other.settings import ACCOUNTS, df_clean_tweets, query_btc_daily, query_btc_hourly, GET_ID, TOOLTIP_INFO
from dashboard.other.serialization import read_serialized
from data_managing.db_handler import retrieve_data
from src.archive.data_managing.preprocessing import daily_pipe


def get_tweets(query):
    return pd.DataFrame(retrieve_data(query=query),
                        columns=['tweet_created', 'text', 'vader_compound', 'tb_polarity', 'tb_subjectivity'])


def get_daily_btc(query):
    return pd.DataFrame(retrieve_data(query),
                        columns=['input_date', 'open_price', 'high_price', 'low_price', 'close_price',
                                 'adj_close_price', 'volume'])


def get_hourly_btc(query):
    return pd.DataFrame(retrieve_data(query),
                        columns=['input_datetime', 'high_price', 'low_price', 'open_price', 'close_price', 'volumeto',
                                 'volumefrom'])


def query_df(dataframe: pd.DataFrame, author_ids):
    valid_conversations = dataframe.loc[dataframe['author_id'].isin(author_ids)]['conversation_id']
    return dataframe.loc[dataframe['conversation_id'].isin(valid_conversations)]


def get_dataframes():
    tweets_df = pd.DataFrame(retrieve_data(query=df_clean_tweets),
                             columns=['tweet_created', 'vader_compound', 'tb_polarity',
                                      'tb_subjectivity', 'author_id', 'conversation_id'])
    btc_daily = get_daily_btc(query_btc_daily)
    btc_hourly = get_hourly_btc(query_btc_hourly)
    return tweets_df, btc_daily, btc_hourly


def parse_input(dataframe: pd.DataFrame, indices):
    acc_names = dataframe.iloc[indices, 1].values.tolist()
    print(dataframe.columns.tolist())
    acc_ids = [GET_ID[name] for name in acc_names]
    return acc_ids, acc_names


def create_dash_table(dataframe: pd.DataFrame, indices):
    df = dataframe[['Account', 'BTC Close', 'BTC Open', 'BTC High', 'BTC Low', 'Count', 'Avg Count']]
    return dash_table.DataTable(
        id='datatable',
        columns=[
            {'name': i, 'id': i, 'presentation': 'markdown'} if i == 'Account'
            else {'name': i, 'id': i} for i in df.columns
        ],
        data=df.to_dict('records'),
        # filter_action='native',
        sort_action='native',
        sort_mode='multi',
        row_selectable='multi',
        selected_rows=indices,  # [0],
        # selected_row_ids=[],  # https://github.com/plotly/dash/issues/185 BUG (returns [None, None, ...])
        page_action='native',
        tooltip={i: {'value': info, 'use_with': 'header'} for i, info in zip(df.columns, TOOLTIP_INFO)},
        markdown_options={'html': False, 'link_target': '_blank'},
        cell_selectable=False,
        # page_current=1,           # SEE IF WORKS
        # page_count=5,

        # STYLING
        css=[
            {'selector': 'table',
             # 'rule': 'table-layout: fixed',
             'rule': 'width: 30%'}
        ],
        style_header={
            'backgroundColor': '#636ef9',
            'color': 'white',
            # 'fontWeight': 'bold'
        },
        style_cell={
            'fontSize': 17,
            'padding': '10px',
            'textAlign': 'center'
        },
        style_data={
            'backgroundColor': 'rgb(71, 123, 244)',
            'color': 'white',
            'whiteSpace': 'normal',
            'height': 'auto',
            'lineHeight': '10px',
            'overflow': 'initial'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(71, 123, 214)',
                'color': 'white'
            }
        ],
        # style_cell_conditional=[
        # {'if': {'column_id': 'Account'},
        #  'width': '30%'},
        # ],
    )


def plot_pie_chart(data, sources):
    df = data.loc[data['Account_Name'].isin(sources)]
    fig = go.Figure()
    labels = df['Account_Name']
    values = df['Count']
    fig.add_traces(
        [
            go.Pie(
                labels=labels,
                values=values,
            )
        ]
    )
    fig.update_layout({
        'title': f'Distribution of Selected Sources',
    })
    return fig


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


def plot_volume_tweet_count_timeseries(data):
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
    fig.update_layout({'title': f'Timeseries for Normalized BTC Volume and Tweet Count from Selected Sources'})
    return fig


def plot_correlation_heatmap(data, values):
    columns = ['avg_vader_compound', 'avg_tb_polarity', 'avg_tb_subjectivity', 'tweet_count', 'volume']
    columns.extend(values)
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr, title='Correlations')
    return fig


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Home", href="#home")),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("Home", header=True),
                dbc.DropdownMenuItem("Daily", href="#"),
                dbc.DropdownMenuItem("Hourly", href="#"),
            ],
            nav=True,
            in_navbar=True,
            label="More",
        ),
    ],
    brand="NavbarSimple",
    brand_href="#",
    color="primary",
    dark=True,
)


def acc_corr_table(corr_with, data_tweets, btc_daily):
    # data_tweets, btc_daily = read_serialized('data_tweets'), read_serialized('btc_daily')
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
        df = daily_pipe(filtered_tweets, btc_daily)
        corr = df[cols].corrwith(df[corr_with], ).apply(lambda x: float(f'{x: .2f}')).to_dict()
        corr_dict['Account'].append(f'[{label}](https://twitter.com/{label})')
        corr_dict['Account_Name'].append(f'{label}')
        corr_dict['BTC Close'].append(corr['close_price'])
        corr_dict['BTC Open'].append(corr['high_price'])
        corr_dict['BTC High'].append(corr['open_price'])
        corr_dict['BTC Low'].append(corr['low_price'])
        corr_dict['Count'].append(df['tweet_count'].sum())
        corr_dict['Avg Count'].append(round(df['tweet_count'].sum() / df.__len__(), 2))
    return pd.DataFrame(corr_dict).sort_values(by='BTC Close', ascending=False).reset_index(drop=True)
