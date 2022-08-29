from dash import html, dcc
from .constants import DROPDOWN_SENTIMENT, TS_DROPDOWN_OPTIONS_X, TS_DROPDOWN_OPTIONS_Y, HM_DROPDOWN_OPTIONS, \
    TS_VOLUME_HOURLY
import dash_bootstrap_components as dbc


def build_tabs():
    return html.Div(
        id='tabs-h',
        className='tabs',
        children=[
            dcc.Tabs(
                id='app-tabs-h',
                value='tab1',
                className='custom-tabs',
                children=[
                    dcc.Tab(
                        id='sources-tab-h',
                        label='Comparison Settings',
                        value='tab1',
                        className='custom-tab',
                        selected_className='custom-tab--selected'
                    ),
                    dcc.Tab(
                        id='charts-tab-h',
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
            html.Div(
                className='float-container',
                children=[
                    html.Div(
                        className='float-child',
                        children=[
                            html.H5('Correlation Table for Each Source'),
                            html.P('Choose sources for further analysis:'),
                            dcc.Dropdown(
                                options=DROPDOWN_SENTIMENT,
                                placeholder='Average VADER Compound per Hour',
                                id='sentiment-table-h-dp',
                                value='avg_vader_compound',
                                clearable=False,
                            ),
                            dcc.Loading(
                                id='loading-corr-table-h',
                                type='circle',
                                fullscreen=True,
                                children=[
                                    html.Div(id='corr-table-div-h')
                                ]
                            ),
                        ]
                    ),
                    html.Div(
                        className='float-child',
                        children=[
                            html.H5(
                                'Distribution of Selected Sources',
                                className='title',
                            ),
                            dcc.Graph(
                                id='pie-chart-h',
                                className='pie-chart',
                                style={'width': '70vh', 'height': '70vh'}
                            )
                        ]
                    ),
                ]
            ),
        ],
    )


def render_tab_2():
    return html.Div(
        children=[
            html.H5(
                'Timeseries for Sentiment and BTC with Moving Averages for Selected Sources',
                className='title',
            ),
            html.Div(
                children=[
                    html.Label('Rolling Mean K'),
                    dcc.Input(
                        id='rolling-window-h',
                        type='number',
                        value=3,
                        min=2,
                        className='rolling-input'
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            html.Br(),
            html.Div(
                children=[
                    dcc.Dropdown(
                        id='ts-dropdown-x-h',
                        options=TS_DROPDOWN_OPTIONS_X,
                        value='avg_vader_compound_norm',
                        clearable=False
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            html.Div(
                children=[
                    dcc.Dropdown(
                        id='ts-dropdown-y-h',
                        options=TS_DROPDOWN_OPTIONS_Y,
                        value='close_price_norm',
                        clearable=False,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            dcc.Graph(id='ts-sentiment-btc-h'),
            html.H5(
                'Timeseries for Normalized BTC Volume and Tweet Count for Selected Sources',
                className='title',
            ),
            html.Div(
                children=[
                    dcc.Dropdown(
                        options=TS_VOLUME_HOURLY,
                        id='ts-volume-dp-h',
                        value='volumeto_norm',
                        clearable=False
                    )
                ]
            ),
            dcc.Graph(id='ts-volume-h'),
            html.H5(
                'Correlation Heatmap for Selected Sources',
                className='title',
            ),
            html.Div(
                children=[
                    dcc.Dropdown(
                        id='hm-dropdown-h',
                        options=HM_DROPDOWN_OPTIONS,
                        value=['close_price'],
                        multi=True
                    )
                ]
            ),
            dcc.Graph(id='heatmap-h')
        ]
    )


def error_modal():
    return dbc.Modal(
        children=[
            dbc.ModalHeader(dbc.ModalTitle('Note:'), close_button=False),
            dbc.ModalBody('At least one source has to be selected!'),
            dbc.ModalFooter(
                dbc.Button('Ok', id='close_error-h', className='close_error', n_clicks=0)
            ),
        ],
        id='not_selected_sources_error-h',
        keyboard=False,
        backdrop='static',
    )


def layout():
    return html.Div(
        id='dash-container-h',
        children=[
            error_modal(),
            build_tabs(),
            html.Div(
                className='tab-content',
                id='tab-content-h',
            ),
            dcc.Store(id='corr-table-storage-h'),
            dcc.Store(id='filtered-data-storage-h')
        ]
    )
