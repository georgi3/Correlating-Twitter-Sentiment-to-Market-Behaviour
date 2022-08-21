from .constants import DROPDOWN_SENTIMENT, HM_DROPDOWN_OPTIONS, TS_DROPDOWN_OPTIONS_X, TS_DROPDOWN_OPTIONS_Y
from dash import html, dcc
import dash_bootstrap_components as dbc


# Content
def build_tabs():
    return html.Div(
        id='tabs-d',
        className='tabs',
        children=[
            dcc.Tabs(
                id='app-tabs-d',
                value='tab1',
                className='custom-tabs',
                children=[
                    dcc.Tab(
                        id='sources-tab-d',
                        label='Comparison Settings',
                        value='tab1',
                        className='custom-tab',
                        selected_className='custom-tab--selected'
                    ),
                    dcc.Tab(
                        id='charts-tab-d',
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
                id="sentiment-table-d-dp",
                value='avg_vader_compound',
                clearable=False,
            ),
            dcc.Loading(
                id='loading-corr-table-daily',
                type='circle',
                fullscreen=True,
                children=[
                    html.Div(id='corr-table-div-d'),
                ]
            ),
            dcc.Graph(id='pie-chart-d'),
        ],
        style={'width': '48%', 'display': 'inline-block'},
    )


def render_tab_2():
    return html.Div(
        children=[
            html.Div(
                children=[
                    dcc.Dropdown(
                        options=TS_DROPDOWN_OPTIONS_X,
                        id="ts-dropdown-x-d",
                        value='avg_vader_compound_norm',
                        clearable=False,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'}
            ),
            html.Div(
                children=[
                    dcc.Dropdown(
                        options=TS_DROPDOWN_OPTIONS_Y,
                        id="ts-dropdown-y-d",
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
                        id='rolling-window-d',
                        type='number',
                        value=3,
                        min=2,
                    ),
                ],
                style={'width': '48%', 'display': 'inline-block'},
            ),
            dcc.Graph(id="ts-sentiment-btc-d"),
            dcc.Graph(id='ts-volume-d'),
            html.Div(
                children=[
                    dcc.Dropdown(
                        id='hm-dropdown-d',
                        options=HM_DROPDOWN_OPTIONS,
                        value=['close_price'],
                        multi=True
                    )
                ]
            ),
            dcc.Graph(id="heatmap-d"),
        ]
    )


def error_modal():
    return dbc.Modal(
        children=[
            dbc.ModalHeader(dbc.ModalTitle('Note:'), close_button=False),
            dbc.ModalBody('At least one source has to be selected!'),
            dbc.ModalFooter(
                dbc.Button('Ok', id='close_error', n_clicks=0)
            ),
        ],
        id='not_selected_sources_error-d',
        keyboard=False,
        backdrop='static',
    )


def layout():
    return html.Div(
        id='dash-container-d',
        children=[
            error_modal(),
            build_tabs(),
            html.Div(
                id='tab-content-d'
            ),
            dcc.Store(id='corr-table-storage-d'),
            dcc.Store(id='filtered-data-storage-d'),
        ]
    )
