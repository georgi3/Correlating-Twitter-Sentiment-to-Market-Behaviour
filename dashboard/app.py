import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from pages.side_bar import sidebar
from dashboard.other.supporting_scripts import navbar


app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])
# app.layout = html.Div(
#     children=[
#         html.Div(
#             [html.Div(dcc.Link(f"{page['name']} - {page['path']}", href=page['relative_path']))
#                 for page in dash.page_registry.values() if not page['name'] in ['Not found 404']
#             ]), dash.page_container]
# )
header = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row(
                    dbc.Col(dbc.NavbarBrand("Title", className="ms-2")),
                    align="center",
                    className="g-0",
                ),
                href=dash.page_registry['pages.daily']['relative_path'],
                style={"textDecoration": "none"},
            ),
            dbc.Row(
                [
                    dbc.NavbarToggler(id="navbar-toggler"),
                    dbc.Collapse(
                        dbc.Nav(
                            [
                                dbc.NavItem(dbc.NavLink("Home")),
                                dbc.NavItem(dbc.NavLink("Page 1")),
                                dbc.NavItem(
                                    dbc.NavLink("Page 2"),
                                    # add an auto margin after page 2 to
                                    # push later links to end of nav
                                    className="me-auto",
                                ),
                                dbc.NavItem(dbc.NavLink("Help")),
                                dbc.NavItem(dbc.NavLink("About")),
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
#
app.layout = html.Div(
    [header, dash.page_container]
)
if __name__ == '__main__':
    app.run(debug=True)
