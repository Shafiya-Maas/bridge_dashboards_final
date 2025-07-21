from dash import dcc, html, Input, Output, State, callback, dash_table, callback_context, no_update, ALL, register_page, page_container
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import mysql.connector
import configparser
import os
from datetime import datetime, timedelta
import calendar
import re   
import json
import sqlalchemy
import warnings
warnings.filterwarnings("ignore") 

dash.register_page(__name__, path="/core")

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

def get_db_engine():
    creds = config['mysql_devcs']
    user = creds.get('user')
    password = creds.get('password')
    host = creds.get('host')
    port = creds.get('port')
    database = creds.get('database')
    engine_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return sqlalchemy.create_engine(engine_str)

# Calculate dates at global scope
current_date = pd.to_datetime("today").date()
six_months_ago = (pd.to_datetime("today") - pd.DateOffset(months=6)).date()

# Time period options
TIME_PERIOD_OPTIONS = [
    {'label': 'Today', 'value': 'today'},
    {'label': 'Yesterday', 'value': 'yesterday'},
    {'label': 'This Week', 'value': 'this_week'},
    {'label': 'Last Week', 'value': 'last_week'},
    {'label': 'This Month', 'value': 'this_month'},
    {'label': 'Last Month', 'value': 'last_month'},
    {'label': 'This Quarter', 'value': 'this_quarter'},
    {'label': 'This Year', 'value': 'this_year'},
]

# Add this at the top of your file with other constants
COLUMN_NAME_MAPPING = {
    'booking_id': 'Booking ID',
    'raw_log_timestamp': 'Booking Date',
    'user_source': 'Source',
    'master_service': 'Service',
    'vehicle_type': 'Vehicle Type',
    'crm_admin_name': 'Person',
    'b2b_shop_name': 'Outlet Name',
    'Activity_Status_Final': 'Activity Status',
    'category_clean': 'Category',
    'comments': 'Comments',
    'city': 'City',
    'service_type': 'Service Type'
}

# Update the get_period_date_range function
def get_period_date_range(period_value):
    today = datetime.today().date()
    six_months_ago = (today - timedelta(days=180))  # Approximately 6 months
    
    if period_value == 'today':
        return today, today
    elif period_value == 'yesterday':
        yesterday = today - timedelta(days=1)
        return max(yesterday, six_months_ago), yesterday
    elif period_value == 'this_week':
        start = today - timedelta(days=today.weekday())
        return max(start, six_months_ago), start + timedelta(days=6)
    elif period_value == 'last_week':
        start = today - timedelta(days=today.weekday() + 7)
        return max(start, six_months_ago), start + timedelta(days=6)
    elif period_value == 'this_month':
        start = today.replace(day=1)
        end = today.replace(day=calendar.monthrange(today.year, today.month)[1])
        return max(start, six_months_ago), end
    elif period_value == 'last_month':
        first = today.replace(day=1)
        last_end = first - timedelta(days=1)
        start = last_end.replace(day=1)
        return max(start, six_months_ago), last_end
    elif period_value == 'this_quarter':
        quarter = (today.month - 1) // 3 + 1
        start = datetime(today.year, 3 * quarter - 2, 1).date()
        end = (datetime(today.year, 3 * quarter, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        return max(start, six_months_ago), end.date()
    elif period_value == 'this_year':
        start = today.replace(month=1, day=1)
        end = today.replace(month=12, day=31)
        return max(start, six_months_ago), end
    return None, None

def fetch_core_data(start_date=None, end_date=None):
    if start_date is None or end_date is None:
        return pd.DataFrame()
    query = """
    WITH LatestComments AS (
        SELECT user_id, book_id, comments, category, status AS comment_status, log AS comment_log
        FROM admin_comments_tbl ac
        WHERE log = (SELECT MAX(log) FROM admin_comments_tbl ac2 WHERE ac2.book_id = ac.book_id)
    )
    SELECT DISTINCT
        a.booking_id,
        CONCAT(UPPER(LEFT(TRIM(a.vehicle_type), 1)), LOWER(SUBSTRING(TRIM(a.vehicle_type), 2))) AS vehicle_type,
        a.status AS booking_status,
        a.booking_status AS booking_status_code,
        a.axle_flag,
        a.flag,
        a.flag_unwntd,
        a.enquiry_flag,
        a.log AS raw_log_timestamp,
        DATE(ADDTIME(a.log, '05:30:00')) AS booking_date,
        b.b2b_check_in_report,
        b.b2b_swap_flag,
        a.service_status AS service_status_code,
        a.city,
        a.service_type,
        c.master_service,
        d.name AS crm_admin_name,
        e.user_source,
        f.activity AS activity_name,
        a.activity_status AS activity_status_code,
        lc.comments,
        lc.category,
        g.b2b_shop_name,
        uv.vehicle_id AS user_vehicle_id,
        a.vech_id,
        uv.id AS vehicle_table_id,
        a.user_veh_id
    FROM go_bumpr.user_booking_tb a
    LEFT JOIN b2b.b2b_booking_tbl b ON a.booking_id = b.gb_booking_id
    LEFT JOIN go_bumpr.go_axle_service_price_tbl c ON a.service_type = c.service_type AND a.vehicle_type = c.type
    LEFT JOIN crm_admin d ON d.crm_log_id = a.crm_update_id
    LEFT JOIN user_vehicle_table uv ON uv.id = a.user_veh_id
    LEFT JOIN go_bumpr.user_source_tbl e ON e.user_source = a.source
    LEFT JOIN go_bumpr.admin_activity_tbl f ON f.id = a.activity_status
    LEFT JOIN b2b.b2b_mec_tbl g ON g.b2b_shop_id = b.b2b_shop_id
    LEFT JOIN LatestComments lc ON a.booking_id = lc.book_id
    WHERE
        a.log BETWEEN 
            SUBTIME(CONCAT(%s, ' 00:00:00'), '05:30:00') AND 
            SUBTIME(CONCAT(%s, ' 23:59:59'), '05:30:00')
        AND (a.mec_id NOT IN (400001, 200018, 200379, 203042, 400974) or a.mec_id is null)
        AND a.user_id NOT IN (21816, 41317, 859, 3132, 20666, 56511, 2792, 128, 19, 7176, 19470, 1, 951, 103699, 113453, 108783, 226, 252884, 189598, 133986, 270162, 298572, 287322, 53865, 289516, 14485, 1678, 30865, 125455, 338469, 9570, 388733, 276771, 392833, 378368, 309341, 299526, 304771, 1935, 22115, 44794, 1031939, 639065, 662228, 965020, 804253, 722759, 378258, 1088113, 1165855, 1165488, 1133076, 1288252, 304783)
        AND a.source NOT IN ('Sulekha Booking', 'Sbi Bookings', 'BTL Booking', 'RSA Bookings', 'nmsa_web', 'Uber')
        AND (a.service_type NOT IN ('Breakdown Assistance', 'Bike Tyre Puncture', 'Car Tyre Puncture', 'Flat Tyre Assistance', 'Vehicle Towing', 'Puncture', 'Towing', 'Bike Breakdown', 'Bike Puncture', 'Deep Clean', 'IOCL Check-up') or a.service_type is null)
        AND a.nmsa_flag != 1
        AND a.flag_unwntd != 1
        AND (b.b2b_swap_flag !=1 or b.b2b_swap_flag is null)
    """
    engine = get_db_engine()
    try:
        df = pd.read_sql(query, engine, params=(start_date, end_date))
        df.fillna(0, inplace=True)
        df.infer_objects(copy=False)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()

def prepare_data(df):
    if df is not None and not df.empty:
        df = df[df['user_source'] != 'Re-Engagement Bookings']
        
        # Clean data
        df['master_service'] = df['master_service'].astype(str).replace(['0', '', ' '], 'No Service Available')
        df['service_type'] = df['service_type'].astype(str).replace(['0', '', ' '], 'No Service Available')
        df['crm_admin_name'] = df['crm_admin_name'].astype(str).replace(['0', '', ' '], 'No Name Available')
        df['user_source'] = df['user_source'].astype(str).replace(['0', '', ' '], 'No Service Available')
        df['activity_name'] = df['activity_name'].astype(str).replace(['0', '', ' '], 'Unknown Status')
        df['city'] = df['city'].astype(str).str.strip().str.title().replace(['0', '', ' '], 'No city Available')
        df['comments'] = df['activity_name'].astype(str).replace(['0', "", ' '], 'Unknown Status')
        
        
        #Convert to date only (no time component)
        df['Dates'] = pd.to_datetime(df['booking_date']).dt.date
        df['booking_date'] = pd.to_datetime(df['booking_date']).dt.date  # Add this line to ensure booking_date is date only
        
        # Status mapping
        condition = [
            df['comments'].isin([
                'Customer called for a status update', 'Done with Local shop', 'Duplicate Booking',
                'Just Enquiry/checking the App', 'Just for Quotation', 'Post Service Escalationn',
                'Price not satisfied/Quotes are too high', 'Testing', 'Wrong Number'
            ]),
            df['comments'].isin([
                'All RNRs are exhausted ', 'Currentlyservice is not needed', 'Not in Chennai/Bangalore/Hyderabad/Trichy',
                'Not Interested', 'Reminded in Whatsapp Images not received', 'Vehicle Sold / No Vehicle'
            ])
        ]
        results = ['Cancelled Booking', 'Other Booking']
        df['Activity_Status_Final'] = np.select(condition, results, default='Unknown Status').astype(str)

        conditions = [
            (df['flag'] == 1),
            (df['flag_unwntd'] == 1),
            ((df['booking_status_code'] == 2) & (df['axle_flag'] == 1) & (df['flag'] == 0)) | (df['service_status_code'] == 'Completed'),
            (df['booking_status_code'].isin([3, 4, 5, 6]) & (df['flag'] == 0)),
            (df['booking_status_code'] == 1) & (df['flag'] == 0),
            (df['booking_status_code'] == 0) & (df['flag'] != 1),
        ]
        results = ['Cancelled', 'Duplicate', 'Goaxled', 'Follow-up', 'Idle', 'Others']
        df['new_status'] = np.select(conditions, results, default='Unknown Status').astype(str)
        
        df['vehicle_type'] = df['vehicle_type'].astype(str).str.strip().str.lower()
        df['vehicle_type'] = df['vehicle_type'].apply(lambda x: x if x in ['4w', '2w'] else 'Others')

    return df

def create_pivot_table(df, index_cols, column_cols, value_col, aggfunc, filter_condition=None):
    if df.empty:
        return pd.DataFrame()
    
    if filter_condition:
        df = df.query(filter_condition)
        if df.empty:
            return pd.DataFrame()
    
    pivot_table_df = df.pivot_table(
        index=index_cols,
        columns=column_cols,
        values=value_col,
        aggfunc=aggfunc,
        margins=True,
        margins_name='Grand Total'
    ).fillna(0).astype(int)
    
    percentage_df = pivot_table_df.div(pivot_table_df['Grand Total'], axis=0) * 100
    percentage_df = percentage_df.round(2)
    
    for col in pivot_table_df.columns:
        if col != 'Grand Total':
            pivot_table_df[col] = pivot_table_df[col].astype(str) + ' (' + percentage_df[col].astype(str) + '%)'
    
    pivot_table_df = pivot_table_df.reset_index()
    
    desired_order = ['Grand Total', 'Goaxled', 'Follow-up', 'Cancelled', 'Others', 'Idle', 'Duplicate']
    available_columns = pivot_table_df.columns.tolist()
    ordered_columns = []
    
    for col in pivot_table_df.columns[:len(index_cols)]:
        ordered_columns.append(col)
    
    for col in desired_order:
        if col in available_columns:
            ordered_columns.append(col)
    
    for col in available_columns:
        if col not in ordered_columns and col not in index_cols:
            ordered_columns.append(col)
    
    pivot_table_df = pivot_table_df[ordered_columns]
    pivot_table_df = pivot_table_df.rename(columns={
        'master_service': 'Service',
        'vehicle_type': 'Type',
        'crm_admin_name': 'Name',
        'user_source': 'Source',
        'Activity_Status_Final': 'Activity status',
        'Grand Total': 'Total Leads',
        'cleaned_category': 'Category'
    })
    
    return pivot_table_df

def create_pivot_table_component(df, index_cols, column_cols, value_col, aggfunc, title, filter_condition=None, table_id_suffix=""):
    pivot_df = create_pivot_table(df, index_cols, column_cols, value_col, aggfunc, filter_condition)
    
    if pivot_df.empty:
        return dbc.Alert("No data available for this view.", color="warning")
    
    table_id = f"pivot-table-{'-'.join(index_cols)}"
    if table_id_suffix:
        table_id += f"-{table_id_suffix}"
    
    columns = [{"name": str(col), "id": str(col)} for col in pivot_df.columns]
    data = pivot_df.to_dict('records')
    
    style_data_conditional = [
        # Style for grand total rows
        {
            'if': {
                'filter_query': f'{{{pivot_df.columns[0]}}} = "Grand Total"'
            },
            'backgroundColor': 'rgba(97, 98, 100, 0.1)',
            'color': '#464B7C',
            'fontWeight': 'bold'
        },

        # Alternate row coloring
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgba(0, 0, 0, 0.01)'
        },
        # Uniform style for all cells
        {
            'if': {'column_id': str(pivot_df.columns[-1])},  # Last column as string
            'borderRight': '1px solid #D1D5DB'
        },
        {
            'if': {'column_id': str(pivot_df.columns[0])},  # First column as string
            'textAlign': 'left',
            'paddingLeft': '15px'
        }
    ]
    
    return html.Div([
        html.H5(title, className="mt-3"),
        dash_table.DataTable(
            id={
                'type': 'pivot-table',
                'index': '-'.join(index_cols),
                'suffix': table_id_suffix
            },
            columns=columns,
            data=data,
            fixed_rows={'headers': True},
            style_table={
                'overflowX': 'auto',
                'overflowY': 'auto',
                'maxHeight': '700px',
                'width': '100%',
                'margin': '1rem 0'
            },
            style_header={
                'backgroundColor': '#F8FAFF',
                'color': '#464B7C',
                'fontWeight': 'bold',
                'textAlign': 'center',
                'border': '1px solid #D1D5DB',
                'padding': '12px',
                'position': 'sticky',
                'top': 0,
                'zIndex': 1
            },
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'borderRight': '1px solid #D1D5DB',
                'borderBottom': '1px solid #D1D5DB',
                'minWidth': '120px', 
                'width': '120px', 
                'maxWidth': '120px',
                'whiteSpace': 'normal',
                'color': '#333',  # Uniform text color
                'cursor': 'default',  # No pointer cursor
                'textDecoration': 'none'  # No underlines
            },
            style_data_conditional=style_data_conditional,
            page_action='none',
        )
    ])

# Initialize with empty data
empty_df = pd.DataFrame()
empty_dict = empty_df.to_dict('records')

layout = dbc.Container([
        
        html.Div([
            html.H1("Core Conversion Panel", 
                    style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px', 'fontWeight':'bold', 'fontSize': '24px'})
        ]),
        
        html.Div([
            # Left-aligned group (date controls)
            html.Div([
                dcc.DatePickerRange(
                    id='log-date-picker',
                    min_date_allowed=six_months_ago,
                    max_date_allowed=current_date,
                    initial_visible_month=current_date,
                    start_date=None,
                    end_date=None,
                    display_format='DD-MMM-YYYY',
                    clearable=True,
                    minimum_nights=0,  # Allows same day selection
                    style={'flex': 1, 'minWidth': '0', 'whiteSpace': 'nowrap', 'border': 'none'}
                ),
        
                dcc.Dropdown(
                    id='sort-order-dropdown',
                    options=TIME_PERIOD_OPTIONS,
                    value=None,
                    clearable=True,
                    placeholder="Select Period",
                    style={'width': '140px', 'minWidth': '130px', 'flexShrink': 0, 'backgroundColor': 'transparent'}
                ),
        
                html.Button("Go", id='date-apply-btn', n_clicks=0, style={
                    'height': '32px',
                    'fontWeight': 'bold',
                    'background': 'transparent',
                    'cursor': 'pointer',
                    'color': '#464B7C'
                }),
            ], style={
                'position': 'relative',
                'zIndex': 1000,
                'display': 'flex',
                'alignItems': 'center',
                'gap': '4px',
                'padding': '6px 10px',
                'border': '1px solid #D1D5DB',
                'borderRadius': '6px',
                'backgroundColor': '#fff',
                'boxShadow': '0 1px 2px rgba(0,0,0,0.05)',
            }),
            
            # Spacer to push Clear Filter to the right
            html.Div(style={'flex': 1}),
            
            # Right-aligned buttons
            html.Div([
                html.Button("Export", id='export-data-btn', n_clicks=0, style={
                    'height': '32px',
                    'fontWeight': 'bold',
                    'background': 'transparent',
                    'cursor': 'pointer',
                    'color': '#5bc0de',
                    'marginRight': '12px'
                }),
                html.Button("Clear Filter", id='clear-filters-btn', n_clicks=0, style={
                    'height': '32px',
                    'fontWeight': 'bold',
                    'background': 'transparent',
                    'cursor': 'pointer',
                    'color': '#d9534f',
                })
            ], style={'display': 'flex'})
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'marginBottom': '20px',
            'width': '100%'
        }),

        dbc.Row([
            dbc.Col([
                html.Label("City"),
                dcc.Dropdown(
                    id='city-filter',
                    options=[],
                    multi=True,
                    placeholder="City",
                    style={"height": "38px"},
                    optionHeight=50  
                )
            ], width=2),
            
            dbc.Col([
                html.Label("Vehicle Type"),
                dcc.Dropdown(
                    id='vehicle-type-filter',
                    options=[],
                    multi=True,
                    placeholder="Vehicle Type",
                    style={"height": "38px"},
                    optionHeight=40  
                )
            ], width=2),
            
            dbc.Col([
                html.Label("Master Service"),
                dcc.Dropdown(
                    id='master-service-filter',
                    options=[],
                    multi=True,
                    placeholder="Master Service",
                    style={"height": "38px"},
                    optionHeight=70,
                )
            ], width=2),
            
            dbc.Col([
                html.Label("Service Type"),
                dcc.Dropdown(
                    id='service-type-filter',
                    options=[],
                    multi=True,
                    placeholder="Service Type",
                    style={"height": "38px"},
                    optionHeight=50  
                )
            ], width=2),
            
            dbc.Col([
                html.Label("Person"),
                dcc.Dropdown(
                    id='crm-admin-filter',
                    options=[],
                    multi=True,
                    placeholder="Person",
                    style={"height": "38px"},
                    optionHeight=50  
                )
            ], width=2),
            
            dbc.Col([
                html.Label("All Bookings"),
                dcc.Dropdown(
                    id='user-source-filter',
                    options=[],
                    multi=True,
                    placeholder="Source",
                    style={"height": "38px"},
                    optionHeight=50   
                )
            ], width=2),
        ], style={'marginBottom': '20px'}),
        
        
        # ...existing code...
        html.Div(
                        dcc.Loading(
                            id="pre-store-loading",
                            type="default",
                            color="#007bff",
                            children=[
                                dcc.Store(id='stored-data', data=empty_dict),
                                dcc.Store(id='filtered-data', data=empty_dict),
                                dcc.Download(id="download-active-tab-data"),
                            ]
                        ),
                        style={
                            "display": "flex",
                            "justifyContent": "center",
                            "alignItems": "end",
                            # "marginTop": "30px"
                        }
                    ),
        dcc.Tabs(
            id="tabs",
            value="tab-service",  # Add default value
                children=[
                    dcc.Tab(
                        label="Service",
                        value="tab-service",  # Add value prop
                        children=[html.Div(id='service-pivot-container')],
                        style={
                            'backgroundColor': '#e0e7ef',
                            'color': '#333',
                            'borderRadius': '8px 8px 0 0',
                            'marginRight': '4px',
                            'padding': '8px 16px',
                        },
                        selected_style={
                            'backgroundColor': '#b6d4fe',
                            'color': '#1a237e',
                            'fontWeight': 'bold',
                            'borderRadius': '8px 8px 0 0',
                            'padding': '8px 16px',
                        }
                    ),
                    dcc.Tab(
                        label="Person",
                        value="tab-person",
                        children=[html.Div(id='person-pivot-container')],
                        style={
                            'backgroundColor': '#e0e7ef',
                            'color': '#333',
                            'borderRadius': '8px 8px 0 0',
                            'marginRight': '4px',
                            'padding': '8px 16px',
                        },
                        selected_style={
                            'backgroundColor': '#b6d4fe',
                            'color': '#1a237e',
                            'fontWeight': 'bold',
                            'borderRadius': '8px 8px 0 0',
                            'padding': '8px 16px',
                        }
                    ),
                    dcc.Tab(
                        label="Source",
                        value="tab-source",
                        children=[html.Div(id='source-pivot-container')],
                        style={
                            'backgroundColor': '#e0e7ef',
                            'color': '#333',
                            'borderRadius': '8px 8px 0 0',
                            'marginRight': '4px',
                            'padding': '8px 16px',
                        },
                        selected_style={
                            'backgroundColor': '#b6d4fe',
                            'color': '#1a237e',
                            'fontWeight': 'bold',
                            'borderRadius': '8px 8px 0 0',
                            'padding': '8px 16px',
                        }
                    ),
                    dcc.Tab(
                        label="Non Conversion",
                        value="tab-non-conversion",
                        children=[html.Div(id='non-conversion-container')],
                        style={
                            'backgroundColor': '#e0e7ef',
                            'color': '#333',
                            'borderRadius': '8px 8px 0 0',
                            'marginRight': '4px',
                            'padding': '8px 16px',
                        },
                        selected_style={
                            'backgroundColor': '#b6d4fe',
                            'color': '#1a237e',
                            'fontWeight': 'bold',
                            'borderRadius': '8px 8px 0 0',
                            'padding': '8px 16px',
                        }
                    ),
                    dcc.Tab(
                        label="Follow-up",
                        value="tab-follow-up",
                        children=[html.Div(id='category-container')],
                        style={
                            'backgroundColor': '#e0e7ef',
                            'color': '#333',
                            'borderRadius': '8px 8px 0 0',
                            'marginRight': '4px',
                            'padding': '8px 16px',
                        },
                        selected_style={
                            'backgroundColor': '#b6d4fe',
                            'color': '#1a237e',
                            'fontWeight': 'bold',
                            'borderRadius': '8px 8px 0 0',
                            'padding': '8px 16px',
                        }
                    ),
            ],
            style={
                'backgroundColor': '#f0f4fa',
                'borderRadius': '8px 8px 0 0',
                'padding': '0.5rem 0.5rem 0 0.5rem',
                'marginBottom': '16px'
            }
        ),
        # ...existing code...
       

        dbc.Modal(
            [
                dbc.ModalHeader("Booking Details"),
                dbc.ModalBody(
                    [
                        html.Div(
                            [
                                dbc.Button(
                                    "Export",
                                    id="download-booking-details-btn",
                                    color="primary",
                                    outline=True,
                                    style={"marginBottom": "10px"},
                                    n_clicks=0,
                                ),
                                dcc.Download(id="download-booking-details-csv"),
                            ]
                        ),
                        dash_table.DataTable(
                            id='booking-details-table',
                            columns=[],
                            data=[],
                            style_table={
                                'overflowX': 'auto',
                                'maxHeight': '400px',
                            },
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px',
                                'whiteSpace': 'normal',
                                'minWidth': '100px',
                                'maxWidth': '200px',
                                'textOverflow': 'ellipsis'
                            },
                            style_cell_conditional=[
                                {
                                    'if': {'column_id': 'S.No'},
                                    'width': '50px',
                                    'minWidth': '50px',
                                    'maxWidth': '50px',
                                    'textAlign': 'center'
                                }
                            ],
                            style_header={
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight': 'bold',
                                'position': 'sticky',
                                'top': 0,
                                'zIndex': 1,
                            },
                            style_data_conditional=[
                                {
                                    'if': {'column_id': 'message'},
                                    'fontStyle': 'italic',
                                    'textAlign': 'center'
                                }
                            ],
                        )
                    ]
                )
            ],
            id="booking-details-modal",
            size="lg",
            centered=True,
            scrollable=True,
            style={
                'display': 'flex',
                'justifyContent': 'center',
                'alignItems': 'center',
            },
        )
    ], fluid=True, style={
        'padding': '20px 30px',
        'maxWidth': '95%',
        'transform':'scale(0.95)',
        'transformOrigin':'top center',
        'margin':'0 auto','overflowX':'visible',
        'boxShadow':'0 0 10px rgba(0,0,0,0.1)','borderRadius':'10px',
        'backgroundColor':'#f9f9f9','marginTop':'8px'
    })



@callback(
    [Output('booking-details-modal', 'is_open'),
     Output('booking-details-table', 'data'),
     Output('booking-details-table', 'columns'),
     Output('booking-details-table', 'style_data_conditional')],
    [Input({'type': 'pivot-table', 'index': ALL, 'suffix': ALL}, 'active_cell')],
    [State('filtered-data', 'data'),
     State({'type': 'pivot-table', 'index': ALL, 'suffix': ALL}, 'data'),
     State({'type': 'pivot-table', 'index': ALL, 'suffix': ALL}, 'columns'),
     State({'type': 'pivot-table', 'index': ALL, 'suffix': ALL}, 'id')],
    prevent_initial_call=True
)
def toggle_booking_details(active_cells, filtered_data, tables_data, tables_columns, table_ids):
    ctx = dash.callback_context
    if not ctx.triggered:
        return [False, [], [], []]

    default_return = [False, [], [], []]

    try:
        triggered_prop_id = ctx.triggered[0]['prop_id'].split('.')[0]
        triggered_id = json.loads(triggered_prop_id)

        if not filtered_data or not any(active_cells):
            return default_return

        # Match clicked table index
        clicked_table_idx = None
        for i, table_id in enumerate(table_ids):
            if table_id['type'] == triggered_id.get('type') and \
               table_id.get('index') == triggered_id.get('index') and \
               table_id.get('suffix') == triggered_id.get('suffix'):
                clicked_table_idx = i
                break

        if clicked_table_idx is None:
            return default_return

        clicked_cell = active_cells[clicked_table_idx]
        if not clicked_cell or clicked_cell.get('row') is None or clicked_cell.get('column_id') is None:
            return default_return

        row_idx = clicked_cell['row']
        column_id = clicked_cell['column_id']
        clicked_table_data = tables_data[clicked_table_idx]
        clicked_table_columns = tables_columns[clicked_table_idx]

        if row_idx >= len(clicked_table_data):
            return default_return

        row_data = clicked_table_data[row_idx]
        

        df = pd.DataFrame.from_records(filtered_data)
        if df.empty:
            return default_return

        first_col_id = clicked_table_columns[0]['id']
        is_grand_total = str(row_data.get(first_col_id)).strip().lower() == 'grand total'

        filter_conditions = []

        if triggered_id.get("suffix") == "followup":
            df['category'] = df['category'].replace(0, 'Follow up')
            df['cleaned_category'] = df['category'].astype(str)
            df.loc[df['cleaned_category'].str.startswith('JD -'), 'cleaned_category'] = 'JD Category'
            df['cleaned_category'] = df['cleaned_category'].str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()
            df = df[df['booking_status_code'].isin([3, 4, 5, 6]) & (df['flag'] == 0)].copy()

            # Always apply column_id filtering if it's not 'Total Leads'
            if column_id and column_id != 'Total Leads':
                if column_id in df['vehicle_type'].unique():
                    filter_conditions.append(df['vehicle_type'] == column_id)
                elif column_id in df.columns:
                    if pd.api.types.is_numeric_dtype(df[column_id]):
                        filter_conditions.append(df[column_id] > 0)
                    else:
                        filter_conditions.append(df[column_id] == row_data.get(column_id))
                elif 'week_label' in df.columns:
                    filter_conditions.append(df['week_label'] == column_id)

            # Only apply row-level category filter if not Grand Total
            if not is_grand_total:
                category_val = row_data.get('cleaned_category') or row_data.get('Category')
                if category_val:
                    filter_conditions.append(df['cleaned_category'] == category_val)


        else:
            if not is_grand_total:
                for col in clicked_table_columns:
                    col_id = col['id']
                    val = row_data.get(col_id)
                    if col_id == 'Service':
                        filter_conditions.append(df['master_service'] == val)
                    elif col_id == 'Name':
                        filter_conditions.append(df['crm_admin_name'] == val)
                    elif col_id == 'Source':
                        filter_conditions.append(df['user_source'] == val)
                    elif col_id == 'Activity status':
                        filter_conditions.append(df['Activity_Status_Final'] == val)
                    elif col_id == 'Type':
                        filter_conditions.append(df['vehicle_type'] == val)
                    elif col_id == 'Category':
                        filter_conditions.append(df['cleaned_category'] == val)
                    elif col_id == 'comments':
                        filter_conditions.append(df['comments'] == val)

            if triggered_id.get('suffix') in ['cancelled', 'other']:
                condition_map = {
                    'cancelled': 'Cancelled Booking',
                    'other': 'Other Booking'
                }
                filter_conditions.append(df['Activity_Status_Final'] == condition_map[triggered_id.get('suffix')])

            if column_id != 'Total Leads':
                status_mapping = {
                    'Goaxled': 'Goaxled',
                    'Follow-up': 'Follow-up',
                    'Cancelled': 'Cancelled',
                    'Others': 'Others',
                    'Idle': 'Idle',
                    'Duplicate': 'Duplicate'
                }
                if column_id in status_mapping:
                    filter_conditions.append(df['new_status'] == status_mapping[column_id])

        # Parse cell value like '10 (100%)'
        cell_value = row_data.get(column_id, '0')
        try:
            cell_value = int(re.search(r'\d+', str(cell_value)).group())
        except:
            cell_value = 0

        if cell_value == 0:
            return [True, [{"message": "No Data Found"}], [{"name": "Message", "id": "message"}], []]

        for condition in filter_conditions:
            df = df[condition]

        if df.empty:
            return [True, [{"message": "No Matching Records Found"}], [{"name": "Message", "id": "message"}], []]

        df.insert(0, 'S.No', range(1, len(df) + 1))
        if 'raw_log_timestamp' in df.columns:
            df['raw_log_timestamp'] = df['raw_log_timestamp'].astype(str).str.replace('T', ' ', regex=False)

        base_columns = [
            {"name": "S.No", "id": "S.No"},
            {"name": "Booking ID", "id": "booking_id"},
            {"name": "Booking Date", "id": "raw_log_timestamp"},
        ]

        table_type = triggered_id.get('index', '').split('-')[0]
        tab_specific_columns = []

        if table_type == 'master_service':
            tab_specific_columns = [
                {"name": "Service", "id": "master_service"},
                {"name": "Vehicle Type", "id": "vehicle_type"},
                {"name": "Outlet Name", "id": "b2b_shop_name"},
                {"name": "Person", "id": "crm_admin_name"},
            ]
        elif table_type == 'crm_admin_name':
            tab_specific_columns = [
                {"name": "Person", "id": "crm_admin_name"},
                {"name": "Service", "id": "master_service"},
                {"name": "Vehicle Type", "id": "vehicle_type"},
                {"name": "Source", "id": "user_source"},
                {"name": "Outlet Name", "id": "b2b_shop_name"},
            ]
        elif table_type == 'user_source':
            tab_specific_columns = [
                {"name": "Source", "id": "user_source"},
                {"name": "Service", "id": "master_service"},
                {"name": "Vehicle Type", "id": "vehicle_type"},
                {"name": "Person", "id": "crm_admin_name"},
                {"name": "Outlet Name", "id": "b2b_shop_name"},
            ]
        elif table_type == 'Activity_Status_Final':
            tab_specific_columns = [
                {"name": "Activity Status", "id": "Activity_Status_Final"},
                {"name": "Comments", "id": "comments"},
                {"name": "Vehicle Type", "id": "vehicle_type"},
                {"name": "Service", "id": "master_service"},
                {"name": "Outlet Name", "id": "b2b_shop_name"},
            ]
        elif triggered_id.get('suffix') == 'followup':
            tab_specific_columns = [
                {"name": "Category", "id": "cleaned_category"},
                {"name": "Vehicle Type", "id": "vehicle_type"},
                {"name": "Service", "id": "master_service"},
                {"name": "Person", "id": "crm_admin_name"},
            ]

        modal_columns = base_columns + tab_specific_columns
        display_columns = [col['id'] for col in modal_columns if col['id'] in df.columns]
        modal_data = df[display_columns].to_dict('records')

        style_data_conditional = [
            {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(248, 248, 248)'},
            {'if': {'column_id': 'S.No'}, 'width': '60px', 'textAlign': 'center'}
        ]

        return [True, modal_data, modal_columns, style_data_conditional]

    except Exception as e:
        return default_return


@callback(
    [Output('stored-data', 'data'),
     Output('log-date-picker', 'start_date'),
     Output('log-date-picker', 'end_date')],
    [Input('date-apply-btn', 'n_clicks'),
     Input('clear-filters-btn', 'n_clicks')],
    [State('log-date-picker', 'start_date'),
     State('log-date-picker', 'end_date'),
     State('sort-order-dropdown', 'value')],
    prevent_initial_call=True
)
def update_stored_data(apply_clicks, clear_clicks, start_date, end_date, period_value):
    try:
        ctx = dash.callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if trigger_id == 'clear-filters-btn':
            return empty_dict, None, None

        if trigger_id == 'date-apply-btn':
            # Calculate 6 months ago date
            six_months_ago = (datetime.today() - timedelta(days=180)).date()
            
            if period_value:
                start_date, end_date = get_period_date_range(period_value)
                # Convert to string in YYYY-MM-DD format
                start_date = start_date.strftime('%Y-%m-%d')
                end_date = end_date.strftime('%Y-%m-%d')
            else:
                # If custom dates are selected, enforce 6-month limit
                if start_date and end_date:
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    
                    if start_date_dt < six_months_ago or end_date_dt < six_months_ago:
                        return empty_dict, None, None
                    
                    # Ensure end date is not before start date
                    if end_date_dt < start_date_dt:
                        end_date_dt = start_date_dt
                        end_date = start_date
                else:
                    # Default to today if no dates selected
                    start_date = datetime.today().date().strftime('%Y-%m-%d')
                    end_date = start_date

            # Fetch data with the enforced date range
            df = fetch_core_data(start_date, end_date)
            df = prepare_data(df)

            if df is None or df.empty or 'vehicle_type' not in df.columns:
                return empty_dict, start_date, end_date

            df_filtered = df[df['vehicle_type'] != 'pv']
            return df_filtered.to_dict('records'), start_date, end_date

        raise dash.exceptions.PreventUpdate
    except Exception as e:
        import traceback
        print("Error in update_stored_data:", e)
        traceback.print_exc()
        return empty_dict, None, None


@callback(
    [Output('city-filter', 'options'),
     Output('vehicle-type-filter', 'options'),
     Output('master-service-filter', 'options'),
     Output('service-type-filter', 'options'),
     Output('crm-admin-filter', 'options'),
     Output('user-source-filter', 'options')],
    [Input('stored-data', 'data')]
)
def update_filter_options(stored_data):
    if not stored_data:
        return [], [], [], [], [], []
    
    df = pd.DataFrame.from_records(stored_data)
    
    def get_options(column):
        unique_values = df[column].astype(str).unique()
        return [{'label': val, 'value': val} for val in sorted(unique_values)]
    
    return (
        get_options('city'),
        get_options('vehicle_type'),
        get_options('master_service'),
        get_options('service_type'),
        get_options('crm_admin_name'),
        get_options('user_source')
    )

@callback(
    Output('filtered-data', 'data'),
    [Input('stored-data', 'data'),
     Input('city-filter', 'value'),
     Input('vehicle-type-filter', 'value'),
     Input('master-service-filter', 'value'),
     Input('service-type-filter', 'value'),
     Input('crm-admin-filter', 'value'),
     Input('user-source-filter', 'value')]
)
def filter_data(stored_data, cities, vehicle_types, master_services, service_types, crm_admins, user_sources):
    if not stored_data:
        return empty_dict
    
    df = pd.DataFrame.from_records(stored_data)
    
    if cities and len(cities) > 0:
        df = df[df['city'].isin(cities)]
    if vehicle_types and len(vehicle_types) > 0:
        df = df[df['vehicle_type'].isin(vehicle_types)]
    if master_services and len(master_services) > 0:
        df = df[df['master_service'].isin(master_services)]
    if service_types and len(service_types) > 0:
        df = df[df['service_type'].isin(service_types)]
    if crm_admins and len(crm_admins) > 0:
        df = df[df['crm_admin_name'].isin(crm_admins)]
    if user_sources and len(user_sources) > 0:
        df = df[df['user_source'].isin(user_sources)]
    
    return df.to_dict('records')

@callback(
    [Output('service-pivot-container', 'children'),
     Output('person-pivot-container', 'children'),
     Output('source-pivot-container', 'children'),
     Output('non-conversion-container', 'children'),
     Output('category-container', 'children')],  # Add this output
    [Input('filtered-data', 'data')],
    [State('log-date-picker', 'start_date'),
     State('log-date-picker', 'end_date')]
)
def update_pivot_tables(filtered_data, start_date, end_date):
    if not filtered_data:
        message = "No data available"
        if start_date and end_date:
            message += f" from {start_date} to {end_date}."
        else:
            message += ". Please select a valid date range and click 'Go'."

        empty_alert = dbc.Alert(message, color="warning", dismissable=False)
        return empty_alert, empty_alert, empty_alert, empty_alert, empty_alert  # Add extra empty_alert

    df = pd.DataFrame.from_records(filtered_data)

    service_pivot = create_pivot_table_component(
        df, ['master_service'], ['new_status'], 'booking_id', 'count',
        "Service-Based Conversion", "vehicle_type != 'pv'"
    )

    person_pivot = create_pivot_table_component(
        df, ['crm_admin_name'], ['new_status'], 'booking_id', 'count',
        "Person-Based Conversion", "vehicle_type != 'pv'"
    )

    source_pivot = create_pivot_table_component(
        df, ['user_source'], ['new_status'], 'booking_id', 'count',
        "Source-Based Conversion", "vehicle_type != 'pv'"
    )

    non_conversion = html.Div([
        html.H4("Non Conversion - Cancelled", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
        create_pivot_table_component(
            df, ['Activity_Status_Final', 'comments'], ['vehicle_type'], 'booking_id', 'count',
            "", "vehicle_type != 'pv' & Activity_Status_Final == 'Cancelled Booking'", "cancelled"
        ),
        html.H4("Non Conversion - Other Booking", style={'fontWeight': 'bold', 'marginBottom': '10px'}),
        create_pivot_table_component(
            df, ['Activity_Status_Final', 'comments'], ['vehicle_type'], 'booking_id', 'count',
            "", "vehicle_type != 'pv' & Activity_Status_Final == 'Other Booking'", "other"
        )
    ])

    # Step 1: Replace category = 0 with "Unknown Category"
    df['category'] = df['category'].replace(0, 'Follow up')

    # Step 2: Clean the category column
    df['cleaned_category'] = df['category'].astype(str)

    # Step 3: Mark JD Categories
    df.loc[df['cleaned_category'].str.startswith('JD -'), 'cleaned_category'] = 'JD Category'

    # Step 4: Remove numeric booking ID prefixes like "3690262 -", but skip "JD Category"
    df['cleaned_category'] = df['cleaned_category'].str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()

    # Filter for follow-up rows
    followup_df = df[(df['booking_status_code'].isin([3, 4, 5, 6])) & (df['flag'] == 0)]


    category_pivot = html.Div([
        html.H4("Follow-up Bookings"),
        create_pivot_table_component(
            followup_df, 
            ['cleaned_category'],  # Group by person - change this as needed
            ['vehicle_type'], 
            'booking_id', 
            'count',
            "",
            "vehicle_type != 'pv'",
            "followup"
        )
    ])    
    
    return service_pivot, person_pivot, source_pivot, non_conversion, category_pivot  # Add category_pivot to return

@callback(
    [Output('city-filter', 'value'),
     Output('vehicle-type-filter', 'value'),
     Output('master-service-filter', 'value'),
     Output('service-type-filter', 'value'),
     Output('crm-admin-filter', 'value'),
     Output('user-source-filter', 'value'),
     Output('sort-order-dropdown', 'value')],
    [Input('clear-filters-btn', 'n_clicks')]
)
def clear_filters(n_clicks):
    if n_clicks and n_clicks > 0:
        return None, None, None, None, None, None, None
    raise dash.exceptions.PreventUpdate

@callback(
    Output("download-booking-details-csv", "data"),
    Input("download-booking-details-btn", "n_clicks"),
    State('booking-details-table', 'data'),
    State('booking-details-table', 'columns'),
    prevent_initial_call=True,
)
def download_booking_details_csv(n_clicks, table_data, table_columns):
    if not n_clicks or not table_data or not table_columns:
        return no_update
    df = pd.DataFrame(table_data)
    col_order = [col['id'] for col in table_columns if col['id'] in df.columns]
    df = df[col_order]

    # Rename columns using the mapping
    df.rename(columns=COLUMN_NAME_MAPPING, inplace=True)
    
    csv_string = df.to_csv(index=False, encoding='utf-8')
    return dict(content=csv_string, filename="booking_details.csv")



@callback(
    Output("download-active-tab-data", "data"),
    Input("export-data-btn", "n_clicks"),
    State('filtered-data', 'data'),
    State('tabs', 'value'),
    prevent_initial_call=True
)
def export_active_tab_data(n_clicks, filtered_data, active_tab):
    if not n_clicks or not filtered_data or not active_tab:
        return no_update
    
    try:
        df = pd.DataFrame.from_records(filtered_data)
        
        # Determine which columns to include based on active tab
        if active_tab == "tab-service":
            pivot_df = create_pivot_table(df, ['master_service'], ['new_status'], 'booking_id', 'count')
            filename = "service_conversion_data.csv"
        elif active_tab == "tab-person":
            pivot_df = create_pivot_table(df, ['crm_admin_name'], ['new_status'], 'booking_id', 'count')
            filename = "person_conversion_data.csv"
        elif active_tab == "tab-source":
            pivot_df = create_pivot_table(df, ['user_source'], ['new_status'], 'booking_id', 'count')
            filename = "source_conversion_data.csv"
        elif active_tab == "tab-non-conversion":
            cancelled_df = df[df['Activity_Status_Final'] == 'Cancelled Booking']
            other_df = df[df['Activity_Status_Final'] == 'Other Booking']
            cancelled_pivot = create_pivot_table(cancelled_df, ['Activity_Status_Final', 'comments'], ['vehicle_type'], 'booking_id', 'count')
            other_pivot = create_pivot_table(other_df, ['Activity_Status_Final', 'comments'], ['vehicle_type'], 'booking_id', 'count')
            pivot_df = pd.concat([cancelled_pivot, other_pivot])
            filename = "non_conversion_data.csv"
        elif active_tab == "tab-follow-up":
            df['category_clean'] = df['category'].astype(str).str.replace(r'^\d+\s*-\s*', '', regex=True).str.strip()
            followup_df = df[df['booking_status_code'].isin([3, 4, 5, 6]) & (df['flag'] == 0)]
            pivot_df = create_pivot_table(followup_df, ['category_clean'], ['vehicle_type'], 'booking_id', 'count')
            filename = "followup_data.csv"
        else:
            return no_update
        
        # Convert to CSV
        csv_string = pivot_df.to_csv(index=False, encoding='utf-8')
        return dict(content=csv_string, filename=filename)

    except Exception as e:
        print(f"Export error: {str(e)}")
        return no_update