import dash
from dash import Dash, html, dcc, Input, Output, State, page_container
import dash_bootstrap_components as dbc

SIDEBAR_WIDTH = 250

# Sidebar styles
SIDEBAR_HIDDEN = {
    "position": "fixed",
    "top": 0,
    "left": f"-{SIDEBAR_WIDTH}px",
    "width": f"{SIDEBAR_WIDTH}px",
    "height": "100%",
    "backgroundColor": "#F8F9FA",
    "padding": "1rem",
    "transition": "left 0.3s ease-in-out",
    "zIndex": "1000",
    "boxShadow": "2px 0px 6px rgba(0,0,0,0.2)",
}
SIDEBAR_VISIBLE = SIDEBAR_HIDDEN.copy()
SIDEBAR_VISIBLE["left"] = "0"

def toggle_button_style(offset):
    return {
        "position": "fixed",
        "top": "1rem",
        "left": f"{offset}px",
        "zIndex": "1100",
        "transition": "left 0.3s ease-in-out",
    }

link_style = {
    "padding": "0.5rem 1rem",
    "color": "#000",
    "textDecoration": "none",
    "display": "block"
}

app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.CERULEAN]
)
server = app.server

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id="login-status", storage_type="session"),
    dcc.Store(id='sidebar-store', data={'open': True}),  # Changed to True for default expanded
    html.Div(id="main-layout")
])

@app.callback(
    Output("main-layout", "children"),
    Input("url", "pathname"),
    State("login-status", "data")
)
def render_protected_layout(pathname, login_status):
    if pathname == "/login":
        from pages import login
        return login.layout
    if not login_status or not login_status.get("logged_in"):
        return dcc.Location(href="/login", id="force-login")
    
    display_name = login_status.get("name") or login_status.get("username") or "Unknown User"
    
    return html.Div([
        dcc.Store(id='sidebar-store', data={'open': True}),  # Changed to True
        html.Button("\u2630", id="toggle-btn", n_clicks=0, style=toggle_button_style(SIDEBAR_WIDTH)),
        html.Div([
            html.H4("Reports"),
            html.Hr(),
            html.Div(
                f"Logged in as: {display_name}",
                style={
                    "marginBottom": "1rem",
                    "fontWeight": "bold",
                    "color": "#007bff",
                    "fontSize": "1rem"
                }
            ),
            dbc.Nav([
                html.Div([
                    dbc.Button(
                        "Core Panel\u25b8",
                        id="btn-collapse-core",
                        color="light",
                        style={"textAlign": "left", "width": "100%", "marginBottom": "0.5rem"},
                        className="nav-custom"
                    ),
                    dbc.Collapse(
                        [
                            dbc.NavLink(
                                "Core Dashboard",
                                href="/core",
                                id="link-core",
                                style={"paddingLeft": "2rem", **link_style},
                                active=False,
                                className="nav-custom"
                            ),
                        ],
                        id="collapse-core",
                        is_open=False
                    )
                ]),
                html.Div([
                    dbc.Button(
                        "Feedback Panel\u25b8",
                        id="btn-collapse-feedback",
                        color="light",
                        style={"textAlign": "left", "width": "100%", "marginBottom": "0.5rem"},
                        className="nav-custom"
                    ),
                    dbc.Collapse(
                        [
                            dbc.NavLink(
                                "Feedback Dashboard",
                                href="/feedback",
                                id="link-feedback",
                                style={"paddingLeft": "2rem", **link_style},
                                active=False,
                                className="nav-custom"
                            ),
                        ],
                        id="collapse-feedback",
                        is_open=False
                    )
                ]),
                html.Div([
                    html.Hr(),
                    dbc.Button("Logout", id="logout-btn", color="danger", size="sm", className="mt-2", style={"width": "100%"} )
                ])
            ], vertical=True, pills=False)
        ], id="sidebar", style=SIDEBAR_VISIBLE),  # Changed to SIDEBAR_VISIBLE
        html.Div(
            id="page-content",
            children=(
                html.Div(
                    [
                        html.H2("Welcome, Bridge Report", style={"marginTop": "2rem", "textAlign": "center", "color": "#464B7C"}),
                        html.H4("Dashboards", style={"textAlign": "center", "color": "#007bff"})
                    ]
                ) if pathname == "/" else page_container
            ),
            style={
                "marginLeft": f"{SIDEBAR_WIDTH + 10}px",  # Adjusted for expanded sidebar
                "padding": "2rem",
                "transition": "margin-left 0.3s ease-in-out"
            }
        )
    ])

@app.callback(
    Output("collapse-core", "is_open"),
    Input("btn-collapse-core", "n_clicks"),
    State("collapse-core", "is_open")
)
def toggle_core(n, is_open):
    if not n:
        raise dash.exceptions.PreventUpdate
    return not is_open

@app.callback(
    Output("collapse-feedback", "is_open"),
    Input("btn-collapse-feedback", "n_clicks"),
    State("collapse-feedback", "is_open")
)
def toggle_feedback(n, is_open):
    if not n:
        raise dash.exceptions.PreventUpdate
    return not is_open

@app.callback(
    [Output("sidebar", "style"),
     Output("toggle-btn", "style"),
     Output("sidebar-store", "data"),
     Output("page-content", "style")],
    Input("toggle-btn", "n_clicks"),
    State("sidebar-store", "data")
)
def toggle_sidebar(n_clicks, store):
    if not n_clicks:
        raise dash.exceptions.PreventUpdate
    is_open = not store["open"]
    sidebar_style = SIDEBAR_VISIBLE if is_open else SIDEBAR_HIDDEN
    button_style = toggle_button_style(SIDEBAR_WIDTH if is_open else 0)
    content_style = {
        "marginLeft": f"{SIDEBAR_WIDTH + 10}px" if is_open else "1rem",
        "padding": "2rem",
        "transition": "margin-left 0.3s ease-in-out"
    }
    return sidebar_style, button_style, {"open": is_open}, content_style

@app.callback(
    Output("link-core", "active"),
    Input("url", "pathname")
)
def highlight_active(pathname):
    return pathname == "/core"

@app.callback(
    Output("login-status", "data", allow_duplicate=True),
    Output("url", "pathname", allow_duplicate=True),
    Input("logout-btn", "n_clicks"),
    prevent_initial_call=True
)
def logout_user(n_clicks):
    if n_clicks:
        return True, "/login"
    raise dash.exceptions.PreventUpdate

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .nav-custom:hover {
                background-color: #E9ECEF !important;
                border-radius: 0.25rem;
                cursor: pointer;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == "__main__":
    app.run(debug=False, port=8501)