from dash import dash_table
import pandas as pd
from supporting_scripts.misc import round_dash_table
import plotly.express as px
import plotly.graph_objects as go

MARGIN = dict(l=30, r=30, t=50, b=30)
BACKGROUND = 'rgb(32,33,47)'


# TODO ADD TITLE to the table
def create_dash_table(dataframe: pd.DataFrame, indices: list, html_id: str, tooltip: list):
    """
    Creates dash table.
    :param dataframe: pd.DataFrame to be converted
    :param indices: list, indices that should be checked
    :param html_id: str, id of the table (has to be unique)
    :param tooltip: list, tooltip information for the columns
    :return: dash_table.DataTable()
    """
    df = dataframe[['Account', 'BTC Close', 'BTC Open', 'BTC High', 'BTC Low', 'Count', 'Avg Count']]
    return dash_table.DataTable(
        id=html_id,
        columns=[
            {'name': i, 'id': i, 'presentation': 'markdown'} if i == 'Account'
            else {'name': i, 'id': i} for i in df.columns
        ],
        data=round_dash_table(df),  # df.to_dict('records')
        sort_action='native',
        sort_mode='multi',
        row_selectable='multi',
        selected_rows=indices,
        # selected_row_ids=[],  # https://github.com/plotly/dash/issues/185 BUG (returns [None, None, ...])
        page_action='native',
        tooltip={i: {'value': info, 'use_with': 'header'} for i, info in zip(df.columns, tooltip)},
        markdown_options={'html': False, 'link_target': '_blank'},
        cell_selectable=False,
        css=[
            {'selector': 'table',
             'rule': 'width: 30%'},
            {'selector': '.dash-table-tooltip',
             'rule': 'background-color: rgb(32,33,47);'
                     'font-family: monospace;'
                     'color: white'}
        ],
        style_header={
            'backgroundColor': 'rgb(239,147,55)',
            'color': 'black',
        },
        style_cell={
            'fontSize': 17,
            'padding': '10px',
            'textAlign': 'center'
        },
        style_data={
            'backgroundColor': 'orange',
            'color': 'black',
            'whiteSpace': 'normal',
            'height': 'auto',
            'lineHeight': '10px',
            'overflow': 'initial'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(233,142,53)',
                'color': 'black'
            }
        ],
    )


# CHARTS
def plot_pie_chart(data: pd.DataFrame, sources: list):
    """
    Plots pie chart.
    :param data: pd.DataFrame, data to be used
    :param sources: list, selected sources
    :return: go.Figure obj
    """
    df = data.loc[data['Account_Name'].isin(sources)]
    fig = go.Figure()
    labels = df['Account_Name']
    values = df['Count']
    fig.add_traces(
        [
            go.Pie(
                labels=labels,
                values=values,
                textinfo='percent+label'
            )
        ]
    )
    fig.update_layout({
        # 'title': is in HTML layout
        "margin": MARGIN,
        "showlegend": False,
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"color": "white"},
        "autosize": False,
    })
    return fig


def plot_sentiment_btc_timeseries(data, btc_price_kind, btc_label, sentiment_kind, sentiment_label, window):
    """
    Plots Sentiment vs Bitcoin timeseries
    :param data: pd.DataFrame, data to be used
    :param btc_price_kind: str, Bitcoin price to be plotted against (name of the column)
    :param btc_label: str, label for the Bitcoin price
    :param sentiment_kind: str, sentiment type to be plotted against (name of the column)
    :param sentiment_label: str, label for sentiment type
    :param window:  int, rolling window
    :return: go.Figure
    """
    fig = go.Figure()
    fig.add_traces([
        go.Scatter(x=data.index, y=data[btc_price_kind], name=f'{btc_label}',
                   mode='lines', line={'dash': 'solid', 'color': 'yellow'}),
        go.Scatter(x=data.index, y=data[btc_price_kind].rolling(window).mean(), name=f'MA for {btc_label};'
                                                                                     f'<br> Rolling Window {window}',
                   mode='lines', line={'dash': 'dot', 'color': 'yellow'}),
        go.Scatter(x=data.index, y=data[sentiment_kind], name=f'{sentiment_label}',
                   mode='lines', line={'dash': 'solid', 'color': 'Orange'}),
        go.Scatter(x=data.index, y=data[sentiment_kind].rolling(window).mean(), name=f'MA for {sentiment_label};'
                                                                                     f'<br> Rolling Window {window}',
                   mode='lines', line={'dash': 'dot', 'color': 'Orange'}),
    ])
    corr_reg = pd.concat([data[btc_price_kind], data[sentiment_kind]], axis=1).corr().iloc[1, 0]
    corr_rol = pd.concat([data[btc_price_kind].rolling(window).mean(), data[sentiment_kind].rolling(window).mean()],
                         axis=1).corr().iloc[1, 0]
    text = f'Solid Lines Corr: <b>{corr_reg: .3f}</b><br>' \
           f'Dotted Lines Corr: <b>{corr_rol: .3f}</b>'
    # rgb(164,222,211)
    fig_layout = go.Layout(
        xaxis=go.layout.XAxis(
            domain=[0, 1],
            showgrid=False,
            gridcolor='rgb(1,0,230)'
        ),
        yaxis=go.layout.YAxis(
            showgrid=True,
            gridcolor='rgb(1,0,230)'
        ),
        #  title = is in HTML layout
        # title=f'Timeseries for {btc_label} and {sentiment_label}',
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
        ],
        margin=MARGIN,
        showlegend=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={'color': 'white'},
        autosize=True
    )
    fig.layout = fig_layout
    return fig


def plot_volume_tweet_count_timeseries(data: pd.DataFrame, volume_kind='volume_norm'):
    """
    Plots Volume of Bitcoin vs Tweet Count Timeseries
    :param data: pd.DataFrame, data to be displayed
    :param volume_kind: str, volume kind (because different API are used for daily and hourly info for BTC)
    :return: go.Figure
    """
    fig = go.Figure()
    # marker={'bgcolor': 'rgb(1,0,230)'}
    fig.add_traces([
        go.Scatter(x=data.index, y=data[volume_kind], name='BTC Volume Normalized', mode='lines',
                   line={'dash': 'solid', 'color': 'Orange'}),
        go.Scatter(x=data.index, y=data['high_price_norm'], name='BTC High Normalized', mode='lines',
                   line={'dash': 'solid', 'color': 'yellow'}),
        go.Scatter(x=data.index, y=data['tweet_count_norm'], name='Tweet Count Normalized', mode='lines',
                   line={'dash': 'dot', 'color': 'rgb(161,102,242)'}),
        go.Bar(x=data.index, y=data['tweet_count_norm'], name='Tweet Count Normalized'),
    ])
    fig_layout = go.Layout(
        yaxis=go.layout.YAxis(
            showgrid=True,
            gridcolor='rgb(1,0,230)'
        ),
        # title=f'Timeseries for Normalized BTC Volume and Tweet Count from Selected Sources',
        margin=MARGIN,
        showlegend=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={'color': 'white'},
        autosize=True
    )
    fig.layout = fig_layout
    return fig


def plot_correlation_heatmap(data: pd.DataFrame, values: list, columns: list):
    """
    Plots Correlation Heatmap
    :param data: pd.DataFrame, data to be used
    :param values: list, extra columns that are chosen
    :param columns: list, of the constant columns to be used
    :return: px.imshow
    """
    columns.extend(values)
    corr = data[columns].corr(method='pearson')
    fig = px.imshow(corr)
    fig.update_layout({
        "margin": MARGIN,
        "showlegend": True,
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {"color": "white"},
        "autosize": True
    })
    return fig
