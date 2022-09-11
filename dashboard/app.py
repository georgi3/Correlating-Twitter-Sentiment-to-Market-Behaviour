import dash_bootstrap_components as dbc
from dash import Dash, html
import dash
import flask
import datetime
# import manage_serialization


# TODO: 1. Add select all to datatable  https://github.com/plotly/dash-table/issues/249#issuecomment-693131768
#       2. Add DataRange  https://dash.plotly.com/dash-core-components/datepickerrange
#       Performance:  https://dash.plotly.com/performance
#       Examples: https://dash.gallery/dash-manufacture-spc-dashboard/
#       Sharing Data: https://dash.plotly.com/sharing-data-between-callbacks
#       Building Components: https://dash.plotly.com/react-for-python-developers
#       Embedding Dash: https://dash.plotly.com/integrating-dash

# server = flask.Flask(__name__)
# server.secret_key = ';lmdflksngnskgnpafponf'
# server.config['DEBUG'] = True
app = Dash(__name__,
           use_pages=True,
           external_stylesheets=[dbc.themes.BOOTSTRAP],
           meta_tags=[
               {
                   "name": "viewport",
                   "content": "width=device-width, initial-scale=1"
               }
           ],
           # server=server
           )
app.title = 'BTC vs Tweets'
app.config['suppress_callback_exceptions'] = True


header = dbc.Navbar(
    dbc.Container(
        [
            dbc.Row(
                [
                    dbc.NavbarToggler(id="navbar-toggler"),
                    dbc.Collapse(
                        dbc.Nav(
                            [
                                dbc.NavItem(dbc.NavLink('Home',
                                                        href=dash.page_registry['pages.home']['relative_path'])),
                                dbc.NavItem(dbc.NavLink("Daily",
                                                        href=dash.page_registry['pages.daily']['relative_path'])),
                                dbc.NavItem(dbc.NavLink("Hourly",
                                                        href=dash.page_registry['pages.hourly']['relative_path'])),
                            ],
                            className="w-100",
                        ),
                        id="navbar-collapse",
                        is_open=False,
                        navbar=True,
                    ),
                ],
                className="flex-grow-1",
            ),
        ],
        fluid=True,
    ),
    dark=True,
    color='rgb(32,33,47)',
)
app.layout = html.Div(
    [header, dash.page_container]
)


if __name__ == '__main__':
    print('Dash started running at: ', datetime.datetime.now().strftime('%d/%m/%Y %H:%M'))
    app.run_server(debug=True)
