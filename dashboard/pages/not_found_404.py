import dash
from dash import html

dash.register_page(__name__)

layout = html.H1('Error 404: Page Not Found', style={'text-align': 'center'})
