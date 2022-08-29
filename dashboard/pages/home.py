import dash
from dash import html, dcc

dash.register_page(__name__, path='/')

layout = html.Div(
    className='home',
    children=[
        html.H3(
            'Correlating Twitter Sentiment to Market Behaviour of Bitcoin',
            className='title'
        ),
        html.Br(),
        html.H6('Tweets are gathered hourly from the wall conversations of 12 sources:'),
        html.Ul(
            children=[
                html.Li('Bitcoin'),
                html.Li('BitcoinMagazine'),
                html.Li('BitcoinFear'),
                html.Li('binance'),
                html.Li('BTC_Archive'),
                html.Li('BT'),
                html.Li('cz_binance'),
                html.Li('CoinDesk'),
                html.Li('DocumentingBTC'),
                html.Li('MartiniGuyYT'),
                html.Li('TheCryptoLark'),
                html.Li('Sheldon_Sniper'),
            ]
        ),
        html.P('Tweets are filtered out to avoid spams, preprocessed  and then analyzed using using three techniques.'
                'TextBlob Polarity, TextBlob Subjectivity and VADER (Valence Aware Dictionary sEntiment Reasoner) '
                'Compound. After the scores are calculated, tweets are grouped by time intervals (daily/hourly) and '
                'average statistics is computed per interval. The results are available in Daily and Hourly pages.')
    ]
)
