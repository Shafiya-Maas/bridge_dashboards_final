#perfect layout

import dash
from dash import dcc, html, Input, Output, State, callback, dash_table, callback_context, no_update, ALL, register_page, page_container, dash_table
import dash_bootstrap_components as dbc 
import pandas as pd
from dash.exceptions import PreventUpdate
from datetime import datetime as dt, timedelta
import mysql.connector
import configparser
import os
import sqlalchemy
import warnings
warnings.filterwarnings("ignore")

dash.register_page(__name__, path="/feedback")

# Read database configuration from config.ini
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

def get_db():
    try:
        creds = config['mysql_dev']
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

def get_db_engine():
    creds = config['mysql_dev']
    user = creds.get('user')
    password = creds.get('password')
    host = creds.get('host')
    port = creds.get('port')
    database = creds.get('database')
    engine_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return sqlalchemy.create_engine(engine_str)

# Calculate default date range (last 6 months)
end_date = dt.today().date()
start_date = end_date - timedelta(days=180)

# Load data from database using the new query
def load_feedback_data(start_date=None, end_date=None):
    if start_date is None or end_date is None:
       return pd.DataFrame()  # Return empty DataFrame if no dates provided
    query = """
    WITH LatestBookings AS (
        SELECT DISTINCT
            b.gb_booking_id AS gb_booking_id,
            b.b2b_cust_phone AS customer_number,
            b.b2b_vehicle_type AS b2b_vehicle_type,
            b.b2b_swap_flag AS b2b_swap_flag,
            b.b2b_service_type AS b2b_service_type,
            b.tvs_job_card_no AS tvs_job_card_no,
            b.b2b_log as goaxle_date,
            b.brand as make,
            b.model,
            g.booking_id AS g_booking_id,
            g.mec_id AS mec_id,
            g.status AS g_status,
            g.flag AS g_flag,
            g.booking_status AS g_booking_status,
            g.source AS g_source,
            g.log AS booking_date,
            g.axle_flag AS g_axle_flag,
            g.city AS g_city,
            g.locality AS locality,
            g.flag_unwntd AS flag_unwntd,
            g.flag_duplicate AS flag_duplicate,
            g.service_status AS g_service_status,
            f.b2b_booking_id AS f_b2b_booking_id,
            f.crm_goaxle_id AS crm_goaxle_id,
            f.log AS f_log,
            s.b2b_acpt_flag AS b2b_acpt_flag,
            cm.crm_log_id AS crm_log_id,
            cm.name AS cm_name,
            cm.flag AS cm_flag,
            cm.crm_flag AS crm_flag,
            cm.cre_flag AS cre_flag,
            c.b2b_log AS b2b_log,
            ms.master_service AS ms_master_service
        FROM
            b2b.b2b_booking_tbl AS b
                LEFT JOIN go_bumpr.user_booking_tb AS g ON b.gb_booking_id = g.booking_id
                LEFT JOIN go_bumpr.go_axle_service_price_tbl AS ms ON ms.service_type = g.service_type AND g.vehicle_type = ms.type
                LEFT JOIN go_bumpr.feedback_track AS f ON f.b2b_booking_id = b.b2b_booking_id
                JOIN b2b.b2b_status s ON s.b2b_booking_id = b.b2b_booking_id
                LEFT JOIN go_bumpr.crm_admin cm ON cm.crm_log_id = f.crm_goaxle_id
                LEFT JOIN b2b.b2b_mec_tbl AS m ON m.b2b_shop_id = b.b2b_shop_id
                LEFT JOIN b2b.b2b_checkin_report AS c ON b.b2b_booking_id = c.b2b_booking_id
        WHERE 
            DATE(c.b2b_log) BETWEEN %s AND %s            
            AND
            (
                (b.b2b_check_in_report = 1  OR  g.service_status IN ('Completed', 'inprogress'))
            )
    )
    SELECT * FROM LatestBookings
    """
    engine = get_db_engine()
    try:
        df = pd.read_sql(query, engine, params=(start_date, end_date))
        df.fillna({'datetime_column': pd.NaT}, inplace=True)
        df.infer_objects(copy=False)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

# Column name mapping for display purposes
COLUMN_NAMES_MAPPING = {
    'gb_booking_id': 'Booking ID',
    'b2b_vehicle_type': 'Vehicle Type',
    'g_source': 'Source',
    'ms_master_service': 'Service Type',
    'booking_date': 'Booking Date',
    'goaxle_date': 'Goaxle Date',
    'b2b_log': 'Checkin Date',
    'tvs_job_card_no': 'Job Card No',
    'g_city': 'City',
    'g_service_status': 'Service Status',
    'cm_name': 'Person Name',
    'service_category': 'Service Category',
    'source': 'Source',
    'count': 'Total Checkins',
    'make': 'Make',
    'model': 'Model',
    'customer_number': 'Customer Number'
}

# Load and prepare data
df = load_feedback_data(start_date, end_date)
df = df.copy()  # Add this line
df['b2b_log'] = pd.to_datetime(df['b2b_log']).dt.date
df['service_category'] = df['ms_master_service'].str.extract(r'^(.*?)(?:\s\d+|$)')[0].fillna('Other')
df['source'] = df['g_source'].str.replace('_', ' ').str.title()

def create_grouped_table(df, group_col):
    return df.groupby(group_col).agg(
        count=('gb_booking_id', 'count')
    ).reset_index().sort_values('count', ascending=False)

def create_checklist_options(values, label_prefix):
    """Create checklist options with consistent sorting for mixed types."""
    clean_values = [s for s in values if s is not None and s != 0]  # Optional: Also filter out 0 if needed
    return [{'label': str(s), 'value': s} for s in sorted(clean_values, key=lambda x: str(x))]

# Initialize with empty options - will be populated after date selection
source_options = []
service_options = []
name_options = [] if 'cm_name' in df.columns else []

layout = dbc.Container([
    html.Div([
        html.H4("Feedback Dashboard", className="text-center my-4", style={'color': '#333'}),
        dbc.Button(
            "Export Data",
            id="export-pivot-btn",
            color="primary",
            outline=True,
            style={
                "position": "absolute",
                "right": "30px",
                "top": "10px",
                "fontSize": "14px"
            }
        ),
    ], style={"position": "relative"}),
    dcc.Store(id="scale-value", data=0.7),
    dcc.Download(id="download-pivot-csv"),
    
    # Filter Row
    html.Div(
        html.Div([
            # Date Range Picker
            dcc.DatePickerRange(
                id='date-range',
                min_date_allowed=start_date,
                max_date_allowed=end_date,
                initial_visible_month=end_date,
                start_date=None,
                end_date=None,
                display_format='YYYY-MM-DD',
                minimum_nights=0,  # Allows same day selection
                style={'minWidth': '0'},            
                ),
            
            # Quick Date Filters
            dcc.Dropdown(
                id="quick-date-dropdown",
                options=[
                    {"label": "Today", "value": "today"},
                    {"label": "Yesterday", "value": "yesterday"},
                    {"label": "Last 7 Days", "value": "last_week"},
                    {"label": "This Month", "value": "this_month"},
                    {"label": "Last Quarter", "value": "last_quarter"},
                    {"label": "This Year", "value": "this_year"},
                ],
                placeholder="Select Period",
                style={
                    "marginRight": "16px",
                    "width": "140px",
                    "padding": "3px 6px",
                    "fontSize": "16px",
                    'flexShrink': 0, 'backgroundColor': 'transparent'
                },
                clearable=True,
            ),
            
            # Apply Date Button
            dbc.Button(
                "GO",
                id="date-go-btn",
                color="primary",
                size="sm",
                style={
                    "marginRight": "16px",
                    "width": "100px",
                    "height": "38px",
                    "fontSize": "14px"
                }
            ),
            
            # Source Filter
            dbc.DropdownMenu(
                label="Select Sources",
                id="source-dropdown",
                children=[
                    dbc.Checklist(
                        id="source-checklist",
                        options=source_options,
                        value=[],
                        inline=False,
                        className="px-2",
                        style={
                            'maxHeight': '140px',
                            'overflowY': 'auto',
                            'fontSize': '12px'
                        }
                    ),
                ],
                className="mb-0",
                style={
                    'marginRight': '16px',
                    'borderRadius': '4px',
                    'width': '140px',
                    'height': '38px',
                },
                color="light"
            ),
            
            # Service Filter
            dbc.DropdownMenu(
                label="Select Service",
                id="service-dropdown",
                children=[
                    dbc.Checklist(
                        id="service-checklist",
                        options=service_options,
                        value=[],
                        inline=False,
                        className="px-2",
                        style={
                            'maxHeight': '140px',
                            'overflowY': 'auto',
                            'fontSize': '12px'
                        }
                    ),
                ],
                className="mb-0",
                style={
                    'marginRight': '16px',
                    'borderRadius': '4px',
                    'width': '140px',
                    'height': '38px',
                },
                color="light"
            ),
            
            # Name Filter
            dbc.DropdownMenu(
                label="Select Names",
                id="name-dropdown",
                children=[
                    dbc.Checklist(
                        id="name-checklist",
                        options=name_options,
                        value=[],
                        inline=False,
                        className="px-2",
                        style={
                            'maxHeight': '140px',
                            'overflowY': 'auto',
                            'fontSize': '12px'
                        }
                    ),
                ],
                className="mb-0",
                style={
                    'marginRight': '16px',
                    'borderRadius': '4px',
                    'width': '140px',
                    'height': '38px',
                },
                color="light"
            ),
            
            # Clear All Button
            dbc.Button(
                "Clear All",
                id="clear-filters-btn",
                color="secondary",
                style={
                    "width": "100px",
                    "height": "38px",
                    "fontSize": "14px",
                }
            )
        ], style={
            "display": "flex",
            "flexDirection": "row",
            "alignItems": "center",
            "gap": "0px",
            "width": "100%",
            "padding": "12px",
            "backgroundColor": "white",
            "borderRadius": "8px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
        }),
        id="scaled-filter-row",
        style={
            "marginBottom": "16px"
        }
    ),
    
    # Alert Messages
    dbc.Alert(
        "Please select a date range and click the GO button to view data.",
        id="date-alert-message",
        color="info",
        style={
            "textAlign": "center",
            "fontSize": "16px",
            "margin": "16px auto",
            "backgroundColor": "#e9f5ff",
            "border": "1px solid #b6e0fe",
            "borderRadius": "8px",
            "padding": "16px"
        }
    ),
    
    dbc.Alert(
        id="no-data-message",
        children="Please select a date range and click the Apply Date button to view data.",
        color="info",
        style={
            "textAlign": "center",
            "fontSize": "16px",
            "margin": "16px auto",
            "backgroundColor": "#e9f5ff",
            "border": "1px solid #b6e0fe",
            "borderRadius": "8px",
            "padding": "16px",
            "display": "none"
        }
    ),
    
    # Main Content Area
    html.Div(
        dcc.Loading(
            id="loading-table",
            type="circle",
            children=[
                # Tabs
                html.Div(
                    dcc.Tabs(
                        id='main-tabs',
                        value='service',
                        children=[
                            dcc.Tab(
                                label='Service Type Wise', 
                                value='service',
                                style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': '#6c757d',
                                    'color': 'white'
                                },
                                selected_style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': "#007bff",
                                    'color': 'white',
                                }
                            ),
                            dcc.Tab(
                                label='Booking Source Wise', 
                                value='source',
                                style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': '#6c757d',
                                    'color': 'white'
                                },
                                selected_style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': "#007bff",
                                    'color': 'white',
                                }
                            ),
                            dcc.Tab(
                                label='Person Wise', 
                                value='cm',
                                style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': '#6c757d',
                                    'color': 'white'
                                },
                                selected_style={
                                    'fontSize': '14px',
                                    'padding': '8px',
                                    'border': 'none',
                                    'backgroundColor': "#007bff",
                                    'color': 'white',
                                }
                            ),
                        ],
                        style={
                            'marginBottom': '16px'
                        }
                    ),
                    id="scaled-tabs",
                    style={"display": "none"}
                ),
                
                # Data Table
                html.Div(
                    dash_table.DataTable(
                        id='data-table',
                        columns=[{"name": "Service Category", "id": "service_category"}],
                        data=[],
                        style_table={
                            'overflowX': 'auto',
                            'width': '100%',
                            'height': '600px',
                            'overflowY': 'auto',
                            'border': '1px solid #ddd',
                            'borderRadius': '4px',
                            'zIndex': '0'
                            
                        },
                        style_header={
                            'backgroundColor': '#f8f9fa',
                            'fontWeight': 'bold',
                            'position': 'sticky',
                            'top': 0,
                            'borderBottom': '1px solid #ddd',
                            'zIndex': '0'
                        },
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'minWidth': '100px',
                            'border': '1px solid #eee'
                        },
                        style_data_conditional=[
                            {'if': {'filter_query': '{service_category} = "Grand Total"'}, 
                             'fontWeight': 'bold', 
                             'backgroundColor': '#f8f9fa'},
                            {'if': {'filter_query': '{source} = "Grand Total"'}, 
                             'fontWeight': 'bold', 
                             'backgroundColor': '#f8f9fa'},
                            {'if': {'filter_query': '{cm_name} = "Grand Total"'}, 
                             'fontWeight': 'bold', 
                             'backgroundColor': '#f8f9fa'},
                        ],
                        sort_action='native',
                        page_action='none',
                        fixed_rows={'headers': True},
                        filter_action='none'
                    ),
                    id="scaled-table",
                    style={"display": "none"}
                ),
                
                # Modal Storage
                dcc.Store(id='modal-table-store'),
            ]
        ),
        style={
            "backgroundColor": "white",
            "borderRadius": "8px",
            "padding": "20px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
        }
    ),
    
    # Modal
    html.Div(
        id="modal",
        style={"display": "none", "zIndex": 2000},
        children=[
            html.Div(
                style={
                    "position": "fixed",
                    "top": "50%",
                    "left": "50%",
                    "transform": "translate(-50%, -50%)",
                    "width": "1200px",
                    "maxWidth": "90vw",
                    "maxHeight": "80vh",
                    "backgroundColor": "white",
                    "padding": "24px",
                    "borderRadius": "8px",
                    "boxShadow": "0 8px 32px rgba(0,0,0,0.25)",
                    "zIndex": 2100, 
                    "overflowY": "auto",
                },
                children=[
                    html.Div(
                        style={
                            "display": "flex",
                            "justifyContent": "space-between",
                            "alignItems": "center",
                            "marginBottom": "16px"
                        },
                        children=[
                            html.H3(id="modal-title", style={"margin": "0"}),
                            html.Button(
                                "✕",
                                id="close-modal",
                                n_clicks=0,
                                style={
                                    "padding": "5px 10px",
                                    "cursor": "pointer",
                                    "backgroundColor": "transparent",
                                    "border": "none",
                                    "fontSize": "20px",
                                    "fontWeight": "bold"
                                }
                            )
                        ]
                    ),
                    html.Div([
                        dbc.Button(
                            "Export",
                            id="download-modal-csv-btn",
                            color="primary",
                            outline=True,
                            style={"marginBottom": "16px"},
                            n_clicks=0,
                        ),
                        dcc.Download(id="download-modal-csv"),
                    ]),
                    html.Div(id="modal-content"),
                ]
            ),
            html.Div(
                id="modal-overlay",
                n_clicks=0,
                style={
                    "position": "fixed",
                    "top": "0",
                    "left": "0",
                    "width": "100%",
                    "height": "100%",
                    "zIndex": 2000,
                    "backgroundColor": "rgba(0,0,0,0.5)"
                }
            )
        ]
    ),
    
    # Data Stores
    dcc.Store(id='filtered-data', data=df.to_dict('records')),
    dcc.Store(id='filters-applied', data=False),
    dcc.Store(id='date-filter-applied', data=False)
], fluid=True, style={
    'padding': '20px 30px',
        'maxWidth': '95%',
        'transformOrigin':'top center',
        'margin':'0 auto','overflowX':'visible',
        'boxShadow':'0 0 10px rgba(0,0,0,0.1)','borderRadius':'10px',
        'backgroundColor':'#f9f9f9','marginTop':'-30px'
    })

# Callback for quick date filters
@callback(
    Output('date-range', 'start_date'),
    Output('date-range', 'end_date'),
    Input('quick-date-dropdown', 'value'),
    prevent_initial_call=True
)
def update_quick_dates_dropdown(selected_value):
    today = dt.today().date()
    if not selected_value:
        return dash.no_update, dash.no_update

    if selected_value == 'today':
        return today, today
    elif selected_value == 'yesterday':
        yest = today - timedelta(days=1)
        return yest, yest
    elif selected_value == 'last_week':
        last_week_start = today - timedelta(days=today.weekday() + 7)
        last_week_end = last_week_start + timedelta(days=6)
        return last_week_start, last_week_end
    elif selected_value == 'this_month':
        start = today.replace(day=1)
        return start, today
    elif selected_value == 'last_quarter':
        month = ((today.month - 1) // 3) * 3 + 1
        last_quarter_month = month - 3 if month > 3 else 10
        last_quarter_year = today.year if month > 3 else today.year - 1
        start = dt(last_quarter_year, last_quarter_month, 1).date()
        end_month = last_quarter_month + 2
        if end_month > 12:
            end_month = 12
        end_day = (dt(last_quarter_year, end_month % 12 + 1, 1) - timedelta(days=1)).day
        end = dt(last_quarter_year, end_month, end_day).date()
        return start, end
    elif selected_value == 'this_year':
        start = today.replace(month=1, day=1)
        return start, today
    return dash.no_update, dash.no_update

# Callback 1: Update dropdown labels and handle checklist selections
@callback(
    [
        Output('source-dropdown', 'label'),
        Output('service-dropdown', 'label'),
        Output('name-dropdown', 'label'),
        Output('source-checklist', 'value'),
        Output('service-checklist', 'value'),
        Output('name-checklist', 'value')
    ],
    [
        Input('source-checklist', 'value'),
        Input('service-checklist', 'value'),
        Input('name-checklist', 'value'),
        Input('clear-filters-btn', 'n_clicks')
    ],
    prevent_initial_call=True
)
def update_labels_and_checklists(source_values, service_values, name_values, clear_clicks):
    ctx = dash.callback_context
    def get_count(values):
        return len(values) if values else 0

    source_label = f"Sources ({get_count(source_values)})" if get_count(source_values) > 0 else "Select Sources"
    service_label = f"Service Types ({get_count(service_values)})" if get_count(service_values) > 0 else "Select Service"
    name_label = f"Names ({get_count(name_values)})" if get_count(name_values) > 0 else "Select Names"

    if ctx.triggered and ctx.triggered[0]['prop_id'].split('.')[0] == 'clear-filters-btn':
        return source_label, service_label, name_label, [], [], []
    return source_label, service_label, name_label, dash.no_update, dash.no_update, dash.no_update

# Callback 2: Filter data based on date selection and update filter options
@callback(
    [
        Output('source-checklist', 'options', allow_duplicate=True),
        Output('service-checklist', 'options', allow_duplicate=True),
        Output('name-checklist', 'options', allow_duplicate=True),
        Output('source-checklist', 'value', allow_duplicate=True),
        Output('service-checklist', 'value', allow_duplicate=True),
        Output('name-checklist', 'value', allow_duplicate=True),
        Output('filtered-data', 'data', allow_duplicate=True),
        Output('date-filter-applied', 'data'),
        Output('date-range', 'start_date', allow_duplicate=True),
        Output('date-range', 'end_date', allow_duplicate=True),
        Output('quick-date-dropdown', 'value', allow_duplicate=True),
        Output('no-data-message', 'children'),
        Output('no-data-message', 'style'),
        Output('scaled-tabs', 'style'),
    ],
    [
        Input('date-go-btn', 'n_clicks'),
        Input('clear-filters-btn', 'n_clicks')
    ],
    [
        State('date-range', 'start_date'),
        State('date-range', 'end_date'),
        State('filtered-data', 'data')
    ],
    prevent_initial_call=True
)
def update_filter_options(date_go_clicks, clear_clicks, start_date, end_date, current_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise PreventUpdate

    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger_id == 'clear-filters-btn':
        return (
    [], [], [], [], [], [], [], False, None, None, None, "", {"display": "none"}, {"display": "none"}
)



    if not (start_date and end_date):
        raise PreventUpdate
    
    filtered_df = df[
        (df['b2b_log'] >= pd.to_datetime(start_date).date()) &
        (df['b2b_log'] <= pd.to_datetime(end_date).date())
    ].copy()

    if filtered_df.empty:
        message = f"No data available from {start_date} to {end_date}"
        return (
            [], [], [], [], [], [],
            [],
            True,
            dash.no_update, dash.no_update, dash.no_update,
            message,
            {"display": "block"},
            {"display": "none"}
        )

    new_source_options = create_checklist_options(filtered_df['source'].dropna().unique(), "Sources")
    new_service_options = create_checklist_options(filtered_df['service_category'].dropna().unique(), "Service Types")
    new_name_options = create_checklist_options(filtered_df['cm_name'].dropna().unique(), "Names") if 'cm_name' in filtered_df.columns else []

    return (
        new_source_options,
        new_service_options,
        new_name_options,
        [], [], [],
        filtered_df.to_dict('records'),
        True,
        dash.no_update,
        dash.no_update,
        dash.no_update,
        "",
        {"display": "none"},
        {"display": "block"}
    )

# Callback 3: Update table data
@callback(
    Output('data-table', 'columns'),
    Output('data-table', 'data'),
    Output('scaled-table', 'style'),
    [
        Input('main-tabs', 'value'),
        Input('filtered-data', 'data'),
        Input('source-checklist', 'value'),
        Input('service-checklist', 'value'),
        Input('name-checklist', 'value'),
        State('date-filter-applied', 'data')
    ]
)
def update_table(selected_table, filtered_data, source_values, service_values, name_values, date_filter_applied):
    if not date_filter_applied:
        return [], [], {"display": "none"}
    
    # Add this line before modifying the filtered DataFrame
    filtered_df = pd.DataFrame(filtered_data).copy()    

    # Apply filters
    if 'source' in filtered_df.columns and source_values:
        filtered_df = filtered_df[filtered_df['source'].isin(source_values)]
    if 'service_category' in filtered_df.columns and service_values:
        filtered_df = filtered_df[filtered_df['service_category'].isin(service_values)]
    if 'cm_name' in filtered_df.columns and name_values:
        filtered_df = filtered_df[filtered_df['cm_name'].isin(name_values)]

    if filtered_df.empty:
        return [], [], {"display": "none"}

    if selected_table == 'service':
        group_column = 'service_category'
    elif selected_table == 'source':
        group_column = 'source'
    else:
        group_column = 'cm_name'

    if group_column not in filtered_df.columns:
        return [], [], {"display": "none"}

    current_table = create_grouped_table(filtered_df, group_column)

    if group_column == "source":
        column_name = "Source"
    elif group_column == "service_category":
        column_name = "Service Category"
    elif group_column == "cm_name":
        column_name = "Person"
    else:
        column_name = group_column.replace('_', ' ').title()

    columns = [
        {"name": column_name, "id": group_column},
        {"name": "Total Checkins", "id": "count"}
    ]

    total = current_table['count'].sum()
    grand_total_row = {
        group_column: 'Grand Total',
        'count': total
    }
    data = current_table.to_dict('records') + [grand_total_row]
    return columns, data, {"display": "block"}

# Callback 4: Handle modal display
@callback(
    Output('modal', 'style'),
    Output('modal-content', 'children'),
    Output('modal-title', 'children'),
    Output('data-table', 'active_cell'),
    Output('modal-table-store', 'data'),
    Output('modal-table-store', 'columns'),
    [Input('data-table', 'active_cell'),
     Input('close-modal', 'n_clicks'),
     Input('modal-overlay', 'n_clicks')],
    [State('data-table', 'data'),
     State('main-tabs', 'value'),
     State('modal', 'style'),
     State('filtered-data', 'data'),
     State('data-table', 'active_cell'),
     State('source-checklist', 'value'),
     State('service-checklist', 'value'),
     State('name-checklist', 'value')],
    prevent_initial_call=True
)
def handle_modal(active_cell, close_clicks, overlay_clicks, table_data, table_type, 
                modal_style, filtered_data, prev_active_cell, source_values, service_values, name_values):
    ctx = dash.callback_context

    if not ctx.triggered:
        raise PreventUpdate

    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger in ['close-modal', 'modal-overlay']:
        return {'display': 'none'}, dash.no_update, dash.no_update, None, dash.no_update, dash.no_update

    if trigger == 'data-table' and active_cell and active_cell['column_id'] == 'count':
        row = table_data[active_cell['row']]
        group_value = row[list(row.keys())[0]];

        # Add this line before modifying the filtered DataFrame
        filtered_df = pd.DataFrame(filtered_data).copy()   

        # Apply all current filters to the modal data
        if 'source' in filtered_df.columns and source_values:
            filtered_df = filtered_df[filtered_df['source'].isin(source_values)]
        if 'service_category' in filtered_df.columns and service_values:
            filtered_df = filtered_df[filtered_df['service_category'].isin(service_values)]
        if 'cm_name' in filtered_df.columns and name_values:
            filtered_df = filtered_df[filtered_df['cm_name'].isin(name_values)]

        if table_type == 'service':
            group_column = 'service_category'
        elif table_type == 'source':
            group_column = 'source'
        else:
            group_column = 'cm_name'

        if group_value == 'Grand Total':
            modal_df = filtered_df.copy()
            title = f"Checkin Details (Grand Total: {len(modal_df)})"
        else:
            modal_df = filtered_df[filtered_df[group_column] == group_value]
            title = f"{group_column.replace('_', ' ').title()}: {group_value}"

        modal_df = modal_df.reset_index(drop=True)
        modal_df.insert(0, "S.No", modal_df.index + 1)

        # Trim 'T' from datetime columns for display
        for col in ["booking_date", "goaxle_date", "b2b_log"]:
            if col in modal_df.columns:
                modal_df[col] = modal_df[col].astype(str).str.replace("T", " ", regex=False)

        # Only keep the required columns in the modal
        modal_columns = [
            "S.No",
            "gb_booking_id",
            "customer_number",
            "b2b_vehicle_type",
            "make",
            "model",
            "g_source",
            "ms_master_service",
            "booking_date",
            "goaxle_date",
            "b2b_log",
            "tvs_job_card_no",
            "g_city",
            "g_service_status",
            "cm_name"
        ]
        # Filter columns that exist in the DataFrame
        modal_columns = [col for col in modal_columns if col in modal_df.columns]
        modal_df = modal_df[modal_columns]

        # Define aliases for display
        column_aliases = {
            "S.No": "S.No",
            "gb_booking_id": "Booking ID",
            "customer_number": "Customer Number",
            "b2b_vehicle_type": "Vehicle Type",
            "make": "Make",
            "model": "Model",
            "g_source": "Source",
            "ms_master_service": "Service Type",
            "booking_date": "Booking Date",
            "goaxle_date": "Goaxle Date",
            "b2b_log": "Checkin Date",
            "tvs_job_card_no": "Job Card No",
            "g_city": "City",
            "g_service_status": "Service Status",
            "cm_name": "Person Name"
        }

        content = html.Div([
            html.P(f"Total Records: {len(modal_df)}"),
            dash_table.DataTable(
                columns=[
                    {"name": column_aliases.get(col, col), "id": col}
                    for col in modal_df.columns
                ],
                data=modal_df.to_dict('records'),
                style_table={
                    'width': '100%',
                    'overflowX': 'auto',
                    'overflowY': 'auto',
                    'marginTop': '16px'
                },
                style_header={
                    'backgroundColor': 'white',
                    'fontWeight': 'bold',
                    'fontSize': '14px',
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '8px',
                    'minWidth': '100px',
                    'maxWidth': '200px',
                    'whiteSpace': 'normal',
                    'fontSize': '14px',
                    'backgroundColor': 'white'
                },
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'S.No'},
                        'minWidth': '60px',
                        'width': '60px',
                        'textAlign': 'center'
                    }
                ],
                page_action='none',
                filter_action='native',
                sort_action='native',
                fixed_rows={'headers': False}
            )
        ])

        return {'display': 'block'}, content, title, None, modal_df.to_dict('records'), modal_columns

    return modal_style, dash.no_update, dash.no_update, None, dash.no_update, dash.no_update

# Callback 5: Show/hide alert based on date filter
@callback(
    Output('date-alert-message', 'style'),
    Input('date-filter-applied', 'data'),
    prevent_initial_call=True
)
def hide_alert(date_filter_applied):
    if date_filter_applied:
        return {"display": "none"}
    return {
        "textAlign": "center",
        "fontSize": "16px",
        "margin": "16px auto",
        "backgroundColor": "#e9f5ff",
        "border": "1px solid #b6e0fe",
        "borderRadius": "8px",
        "padding": "16px"
    }

# Callback 6: Download CSV
@callback(
    Output("download-modal-csv", "data"),
    Input("download-modal-csv-btn", "n_clicks"),
    State("modal-table-store", "data"),
    State("modal-table-store", "columns"),
    prevent_initial_call=True,
)
def download_modal_csv(n_clicks, modal_data, modal_columns):
    if not n_clicks or not modal_data or not modal_columns:
        return dash.no_update
    df = pd.DataFrame(modal_data)
    if isinstance(modal_columns[0], dict) and 'id' in modal_columns[0]:
        col_order = [col['id'] for col in modal_columns if col['id'] in df.columns]
    else:
        col_order = [col for col in modal_columns if col in df.columns]
    df = df[col_order]

    # Rename columns using the mapping
    df.rename(columns=COLUMN_NAMES_MAPPING, inplace=True)
    
    csv_string = df.to_csv(index=False, encoding='utf-8')
    return dict(content=csv_string, filename="checkin_details.csv")


# Callback 7: Export pivot table data
@callback(
    Output("download-pivot-csv", "data"),
    [Input("export-pivot-btn", "n_clicks")],
    [State("data-table", "data"),
     State("data-table", "columns")],
    prevent_initial_call=True
)
def export_pivot_table(n_clicks, table_data, table_columns):
    if not n_clicks or not table_data:
        raise PreventUpdate
    
    # Convert to DataFrame
    df = pd.DataFrame(table_data)
    
    # Get column names from the table columns
    column_names = {col['id']: col['name'] for col in table_columns}
    df = df.rename(columns=column_names)
    
    # Create CSV string
    csv_string = df.to_csv(index=False, encoding='utf-8')
    
    # Return as download
    return dict(content=csv_string, filename="pivot_data.csv")