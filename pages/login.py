import dash
from dash import html, dcc, Output, Input, State, callback, no_update, page_container
import dash_bootstrap_components as dbc
import mysql.connector
import configparser     
import os
from dash.exceptions import PreventUpdate

dash.register_page(__name__, path="/login")

# Read database configuration from config.ini
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

def get_db_connection():
    try:
        creds = config['mysql_devcs']
        return mysql.connector.connect(
            host=creds.get('host'),
            user=creds.get('user'),
            passwd=creds.get('password'),
            database=creds.get('database'),
            port=int(creds.get('port')),
            use_pure=True
        )
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def authenticate_user(agent_id, password):
    """Authenticates user and gets their allowed pages"""
    db = get_db_connection()
    if not db:
        return None
    
    try:
        with db.cursor(dictionary=True) as cursor:
            # Check user credentials and active status
            cursor.execute("""
                SELECT DISTINCT crm_log_id, name 
                FROM user_page_access 
                WHERE crm_log_id = %s AND agent_password = %s AND active_flag = 1
                LIMIT 1
            """, (agent_id, password))
            
            user = cursor.fetchone()
            if not user:
                return False
            
            # Get user's allowed pages
            cursor.execute("""
                SELECT DISTINCT page_path 
                FROM user_page_access 
                WHERE crm_log_id = %s AND active_flag = 1
            """, (agent_id,))
            
            user['allowed_pages'] = [row['page_path'] for row in cursor.fetchall()]
            return user
            
    except Exception as e:
        print(f"Database error: {e}")
        return None
    finally:
        if db and db.is_connected():
            db.close()

# Login Layout
layout = dbc.Container(
    [   
        html.Link(
            rel='stylesheet',
            href='https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css'
        ),
        dbc.Row(
            style={"height": "100vh", "overflow": "hidden"},
            children=[
                dbc.Col(
                    html.Div(
                        html.Img(
                            src="/assets/login_image.png",
                            style={
                                "width": "100%",
                                "height": "100vh",
                                "objectFit": "cover",
                                "display": "block"
                            }
                        ),
                        style={"height": "100vh", "padding": "0", "margin": "0"}
                    ),
                    width=6,
                    style={"padding": "0", "margin": "0"}
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.Div([
                                html.Img(
                                    src="/assets/mytvs-logo.png",
                                    style={"width": "150px", "marginBottom": "20px"}
                                ),
                                html.H2("LOGIN", className="mb-4")
                            ], style={"textAlign": "center"}),
                            dbc.Input(
                                id="username-input", placeholder="Username", type="text",
                                className="mb-3", style={"width": "100%"}
                            ),
                            html.Div(
                                [
                                    dbc.Input(
                                        id="password-input",
                                        placeholder="Password",
                                        type="password",
                                        className="mb-3",
                                        style={
                                            "width": "100%",
                                            "paddingRight": "40px",
                                            "backgroundColor": "#f0f6ff"
                                        }
                                    ),
                                    html.Span(
                                        html.I(id="toggle-password", className="fas fa-eye"),
                                        id="toggle-password-container",
                                        n_clicks=0,
                                        style={
                                            "position": "absolute",
                                            "right": "15px",
                                            "top": "5px",
                                            "cursor": "pointer",
                                            "zIndex": "1000",
                                            "color": "#495057",
                                            "padding": "6px"
                                        }
                                    )
                                ],
                                style={
                                    "position": "relative",
                                    "width": "100%",
                                    "marginBottom": "1rem"
                                }
                            ),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dbc.Checkbox(id="remember-me", label="Remember me"),
                                        width=6
                                    ),
                                    dbc.Col(
                                        html.A("Forgot password?", href="#", style={"textDecoration": "none"}),
                                        width=6,
                                        style={"textAlign": "right"}
                                    )
                                ],
                                className="mb-3",
                                style={"width": "100%"}
                            ),
                            dbc.Button(
                                "Login", id="login-button", color="primary",
                                className="mb-3", style={"width": "100%"}
                            ),
                            html.Div(id="login-message"),
                        ],
                        style={
                            "display": "flex",
                            "flexDirection": "column",
                            "justifyContent": "center",
                            "alignItems": "center",
                            "height": "100vh",
                            "padding": "0 2rem"
                        }
                    ),
                    width=6,
                    style={"padding": "0", "margin": "0", "display": "flex", "alignItems": "center", "justifyContent": "center"}
                )
            ]
        )
    ],
    fluid=True,
    style={"padding": "0", "margin": "0", "minHeight": "100vh", "height": "100vh", "overflow": "hidden"}
)

# Login Callback
@callback(
    Output("login-message", "children"),
    Output("url", "pathname"),
    Output("login-status", "data"),
    Input("login-button", "n_clicks"),
    State("username-input", "value"),
    State("password-input", "value"),
    prevent_initial_call=True
)
def handle_login(n_clicks, username, password):
    if not username or not password:
        return dbc.Alert("Please enter both username and password", color="danger"), no_update, no_update

    user = authenticate_user(username, password)

    if user is None:
        return dbc.Alert("Database connection error", color="danger"), no_update, no_update
    elif user is False:
        return dbc.Alert("Invalid credentials", color="danger"), no_update, no_update
    elif not user.get('allowed_pages'):
        return dbc.Alert("No pages assigned to your account", color="danger"), no_update, no_update
    else:
        # Redirect to first allowed page
        first_allowed_page = user['allowed_pages'][0] if user['allowed_pages'] else "/"
        return (
            dbc.Alert("Login successful! Loading dashboard...", color="success"),
            "/",
            {
                "logged_in": True,
                "username": username,
                "name": user.get("name", username),
                "allowed_pages": user['allowed_pages']
            }
        )

# Password visibility toggle
@callback(
    Output("password-input", "type"),
    Output("toggle-password", "className"),
    Input("toggle-password-container", "n_clicks"),
    State("password-input", "type"),
    prevent_initial_call=True
)
def toggle_password_visibility(n_clicks, current_type):
    if current_type == "password":
        return "text", "fas fa-eye-slash"
    else:
        return "password", "fas fa-eye"

# Main app callback for page access control
@callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
    State("login-status", "data"),
    prevent_initial_call=True
)
def control_page_access(pathname, login_data):
    # Allow access to login page
    if pathname == "/login":
        return layout
    
    # Check if user is logged in
    if not login_data or not login_data.get("logged_in"):
        return dcc.Location(pathname="/login", id="redirect-login")
    
    # Check if user has access to the requested page
    allowed_pages = login_data.get("allowed_pages", [])
    if pathname not in allowed_pages and pathname != "/":
        return dbc.Alert("You don't have permission to access this page", color="danger")
    
    # Return the page content if authorized
    return page_container