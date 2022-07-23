import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from pages.side_bar import sidebar


app = Dash(__name__, use_pages=True)
app.layout = html.Div(
    children=[
        # html.H1('Correlating Twitter Sentiment to Market Behaviour of Bitcoin'),
        # dbc.Col(sidebar(), width=2),
        # dbc.Col(html.Div("Topics Home Page"), width=10),
        html.Div([html.Div(
            dcc.Link(f"{page['name']} - {page['path']}", href=page['relative_path'])
        ) for page in dash.page_registry.values() if not page['name'] in ['Not found 404']
            ]
        ),
        dash.page_container
    ]
)

if __name__ == '__main__':
    app.run(debug=True)
