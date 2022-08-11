import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dashboard.other.supporting_scripts import navbar
from dashboard.other.supporting_scripts import get_dataframes, acc_corr_table
from dashboard.other.serialization import serialize
from apscheduler.schedulers.background import BackgroundScheduler


app = Dash(__name__,
           use_pages=True,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           meta_tags=[
               {
                   "name": "viewport",
                   "content": "width=device-width, initial-scale=1"
               }
           ])
app.title = 'BTC vs Tweets'
app.config['suppress_callback_exceptions'] = True


header = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row(
                    dbc.Col(dbc.NavbarBrand("Home", className="ms-2")),
                    align="center",
                    className="g-0",
                ),
                href=dash.page_registry['pages.home']['relative_path'],
                style={"textDecoration": "none"},
            ),
            dbc.Row(
                [
                    dbc.NavbarToggler(id="navbar-toggler"),
                    dbc.Collapse(
                        dbc.Nav(
                            [
                                dbc.NavItem(dbc.NavLink("Daily",
                                                        href=dash.page_registry['pages.daily']['relative_path'])),
                                dbc.NavItem(dbc.NavLink("Hourly",
                                                        href=dash.page_registry['pages.hourly']['relative_path'])),
                                # dbc.NavItem(dbc.NavLink("Help")),
                                # dbc.NavItem(dbc.NavLink("About")),
                            ],
                            # make sure nav takes up the full width for auto
                            # margin to get applied
                            className="w-100",
                        ),
                        id="navbar-collapse",
                        is_open=False,
                        navbar=True,
                    ),
                ],
                # the row should expand to fill the available horizontal space
                className="flex-grow-1",
            ),
        ],
        fluid=True,
    ),
    dark=True,
    color="blue",
)
app.layout = html.Div(
    [header, dash.page_container]
)


corr_with_values = [
    'avg_vader_compound',
    'avg_tb_subjectivity',
    'avg_tb_polarity',
]


def serialize_db():
    data_tweets, btc_daily, btc_hourly = get_dataframes()
    data_to_serialize = {
        'data_tweets': data_tweets,
        'btc_daily': btc_daily,
        'btc_hourly': btc_hourly,
    }
    indices_mapping = dict()
    for value in corr_with_values:
        corr_table = acc_corr_table(value, data_tweets, btc_daily)
        data_to_serialize[value] = corr_table
        indices_mapping[value] = {
            'indices': {i: name for i, name in zip(corr_table.index, corr_table.Account_Name)},
            'names': {name: i for i, name in zip(corr_table.index, corr_table.Account_Name)}
        }
    data_to_serialize['indices_mapping'] = indices_mapping
    serialize(**data_to_serialize)


if __name__ == '__main__':
    # task scheduler for df retrieval
    scheduler = BackgroundScheduler(timezone='US/Eastern')
    scheduler.add_job(serialize_db, 'interval', hours=1)
    app.run(debug=True)
