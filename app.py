
from dash import Dash
import dash_bootstrap_components as dbc


# Initialize the Dash app
app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)


server = app.server 