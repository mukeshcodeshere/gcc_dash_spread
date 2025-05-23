import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, State, callback_context
import dash_bootstrap_components as dbc
import dash_table
from datetime import datetime as dt, timedelta
import ast
# Assuming seasonalfunctions_sparta.py is in the same directory or accessible
from seasonalFunctions import *
from sqlalchemy import create_engine
from urllib import parse
import time
from io import StringIO # Import StringIO for pandas.read_json deprecation warning

# SQL Connection (for expire data only)
connection_params = {
    "server": "tcp:gcc-db-v100.database.windows.net,1433",
    "database": "GCC-db-100",
    "username": "rrivera",
    "password": "Mistymutt_1",
}

connecting_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={connection_params['server']};"
    f"Database={connection_params['database']};"
    f"Uid={connection_params['username']};"
    f"Pwd={connection_params['password']};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

params = parse.quote_plus(connecting_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

# Load expire data
query = "SELECT * FROM [Reference].[FuturesExpire]"
expire = pd.read_sql(query, con=engine)

# Initialize Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H2("Real-time Seasonal Spread Analysis", className="mb-4"),
    
    # Input Section
    dbc.Card([
        dbc.CardHeader("Spread Configuration"),
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Instrument Name:", className="fw-bold"),
                    dcc.Input(id='name-input', type='text', placeholder='e.g., Crude July Dec', 
                             className='form-control', value='Crude July Dec')
                ], width=3),
                dbc.Col([
                    html.Label("Group:", className="fw-bold"),
                    dcc.Input(id='group-input', type='text', placeholder='e.g., Energy', 
                             className='form-control', value='Energy')
                ], width=2),
                dbc.Col([
                    html.Label("Region:", className="fw-bold"),
                    dcc.Input(id='region-input', type='text', placeholder='e.g., US', 
                             className='form-control', value='US')
                ], width=2),
                dbc.Col([
                    html.Label("Month:", className="fw-bold"),
                    dcc.Input(id='month-input', type='text', placeholder='e.g., May', 
                             className='form-control', value='May')
                ], width=2),
                dbc.Col([
                    html.Label("Roll Flag:", className="fw-bold"),
                    dcc.Input(id='rollflag-input', type='text', placeholder='e.g., HO', 
                             className='form-control', value='HO')
                ], width=3),
            ], className="mb-3"),
            
            dbc.Row([
                dbc.Col([
                    html.Label("Ticker List:", className="fw-bold"),
                    dcc.Input(id='ticker-input', type='text', 
                             placeholder="['/CL', '/CL'] - comma separated, use quotes",
                             className='form-control', value="['/CL', '/CL']")
                ], width=4),
                dbc.Col([
                    html.Label("Contract Months:", className="fw-bold"),
                    dcc.Input(id='contract-months-input', type='text', 
                             placeholder="['M', 'Z'] - comma separated, use quotes",
                             className='form-control', value="['M', 'Z']")
                ], width=4),
                dbc.Col([
                    html.Label("Year Offsets:", className="fw-bold"),
                    dcc.Input(id='year-offset-input', type='text', 
                             placeholder="[0, 0] - comma separated",
                             className='form-control', value="[0, 0]")
                ], width=4),
            ], className="mb-3"),
            
            dbc.Row([
                dbc.Col([
                    html.Label("Weights:", className="fw-bold"),
                    dcc.Input(id='weights-input', type='text', 
                             placeholder="[1, -1] - comma separated",
                             className='form-control', value="[1, -1]")
                ], width=3),
                dbc.Col([
                    html.Label("Conversion Factors:", className="fw-bold"),
                    dcc.Input(id='conv-input', type='text', 
                             placeholder="[1, 1] - comma separated",
                             className='form-control', value="[1, 1]")
                ], width=3),
                dbc.Col([
                    html.Label("Years Back:", className="fw-bold"),
                    dcc.Input(id='years-back-input', type='number', 
                             placeholder='5', className='form-control', value=5)
                ], width=2),
                dbc.Col([
                    html.Label("Description:", className="fw-bold"),
                    dcc.Input(id='desc-input', type='text', 
                             placeholder='Spread description',
                             className='form-control', value='Crude Oil Spread')
                ], width=4),
            ], className="mb-3"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Button("Calculate Spread", id='calculate-btn', color='primary', 
                              className='btn-lg', n_clicks=0)
                ], width=12, className="text-center")
            ])
        ])
    ], className="mb-4"),
    
    # Status and Loading
    dcc.Loading(
        id="loading",
        children=[
            html.Div(id='status-output', className="mb-3"),
            dcc.Graph(id='spread-figure'),
            html.Br(),
            dcc.Graph(id='spread-histogram'),
        ],
        type="default",
    ),
    
    html.H4("Data Preview"),
    dash_table.DataTable(
        id='data-preview',
        page_size=15,
        style_table={'overflowX': 'auto'},
        style_cell={
            'backgroundColor': 'black',
            'color': 'white',
            'textAlign': 'left',
            'fontSize': 12,
        },
        style_header={
            'backgroundColor': 'rgb(30, 30, 30)',
            'fontWeight': 'bold'
        }
    ),
    
    # Store calculated data
    dcc.Store(id='calculated-data')
    
], fluid=True)

def parse_input_list(input_str):
    """Parse string input to list, handling both formats"""
    try:
        return ast.literal_eval(input_str)
    except:
        # Try comma-separated format
        try:
            return [x.strip().strip("'\"") for x in input_str.split(',')]
        except:
            return []

@app.callback(
    Output('calculated-data', 'data'),
    Output('status-output', 'children'),
    Input('calculate-btn', 'n_clicks'),
    State('name-input', 'value'),
    State('group-input', 'value'),
    State('region-input', 'value'),
    State('month-input', 'value'),
    State('rollflag-input', 'value'),
    State('ticker-input', 'value'),
    State('contract-months-input', 'value'),
    State('year-offset-input', 'value'),
    State('weights-input', 'value'),
    State('conv-input', 'value'),
    State('years-back-input', 'value'),
    State('desc-input', 'value'),
    prevent_initial_call=True
)
def calculate_spread_data(n_clicks, name, group, region, month, roll_flag, 
                         ticker_str, contract_months_str, year_offset_str, 
                         weights_str, conv_str, years_back, desc):
    
    if n_clicks == 0:
        return {}, ""
    
    try:
        # Parse inputs
        ticker_list = parse_input_list(ticker_str)
        contract_months_list = parse_input_list(contract_months_str)
        year_offset_list = parse_input_list(year_offset_str)
        weights_list = parse_input_list(weights_str)
        conv_list = parse_input_list(conv_str)
        
        # Convert numeric strings to numbers
        year_offset_list = [int(x) for x in year_offset_list]
        weights_list = [float(x) for x in weights_list]
        conv_list = [float(x) for x in conv_list]
        
        # Generate year list
        year_list = generateYearList(contract_months_list, year_offset_list)
        
        # Get contract data
        prices_dict, expire_list = generate_contract_data_sparta(
            ticker_list, contract_months_list, year_list, 
            weights_list, conv_list, years_back
        )
        
        if not prices_dict:
            return {}, dbc.Alert("No data retrieved for any contracts!", color="danger")
        
        # Validate contract data
        validate_contract_data(prices_dict)
        
        # Build spreads (similar to PriceBuilding_v101.py logic)
        combined_list = [roll_flag + exp for exp in expire_list]
        
        # Filter expire matrix
        expire['TickerMonthYear'] = expire['Ticker'] + expire['MonthCode'] + expire['LastTrade'].str.slice(-2)
        expire_matrix = expire[expire['TickerMonthYear'].isin(combined_list)]
        
        print(f"DEBUG: Combined list: {combined_list}")
        print(f"DEBUG: Expire matrix matches: {len(expire_matrix)}")
        
        spread_dict = {}
        # Get the number of contracts from the first ticker's ContractList
        # This assumes all tickers will have the same number of contracts in their list
        num_contracts = len(next(iter(prices_dict.values()))["ContractList"])
        
        # Iterate over each contract index
        for i in range(num_contracts):
            combined_df = pd.DataFrame()
            first_contracts = []
        
            for ticker, data in prices_dict.items():
                if i < len(data["ContractList"]):
                    first_contract = data["ContractList"][i]
                    first_contracts.append(first_contract)
        
                    temp_df = data["Prices df"][data["Prices df"]['symbol'] == first_contract][["Date", "WeightedPrice"]].copy()
                    print(f"DEBUG: Contract {first_contract} has {len(temp_df)} data points")
                    
                    if temp_df.empty:
                        print(f"WARNING: No data for contract {first_contract}")
                        continue
                        
                    temp_df["Date"] = pd.to_datetime(temp_df["Date"])
                    temp_df.set_index("Date", inplace=True)
                    temp_df.rename(columns={"WeightedPrice": first_contract}, inplace=True)
        
                    if combined_df.empty:
                        combined_df = temp_df
                    else:
                        combined_df = combined_df.join(temp_df, how="outer")
        
            if combined_df.empty:
                print(f"WARNING: No combined data for contract set {i}")
                continue
                
            # Drop rows with missing values and calculate spread
            print(f"DEBUG: Combined DF shape before dropna: {combined_df.shape}")
            combined_df.dropna(inplace=True)
            print(f"DEBUG: Combined DF shape after dropna: {combined_df.shape}")
            
            if combined_df.empty:
                print(f"WARNING: No data after dropna for contract set {i}")
                continue
                
            combined_df["spread"] = combined_df.sum(axis=1, skipna=True)
        
            # Extract year from contract suffix
            if first_contracts:
                year_suffix = first_contracts[0][-2:]
                spread_year = 2000 + int(year_suffix) if int(year_suffix) < 50 else 1900 + int(year_suffix)
                spread_dict[spread_year] = combined_df
                print(f"DEBUG: Added spread data for year {spread_year} with {len(combined_df)} points")
        
        print(f"DEBUG: Spread dict has {len(spread_dict)} years")
        
        # If no expire matrix matches, use all spreads without filtering
        if expire_matrix.empty:
            print("WARNING: No expire matrix matches, using all spread data without filtering")
            filtered_spread_dict = {}
            # Add LastTrade column for each spread
            for year_key, df in spread_dict.items():
                df_copy = df.copy()
                # For historical years, set LastTrade to the max date in the DF for that year
                # This simulates historical expiration if actual expire data is missing
                if year_key <  dt.today().year: # This year and prior are considered historical 
                    df_copy['LastTrade'] = df_copy.index.max()
                else: # Future year, treat as "current" until it becomes historical
                     df_copy['LastTrade'] = pd.Timestamp.today() # Default to today if no specific future expire
                filtered_spread_dict[year_key] = df_copy
        else:
            # Filter spreads based on expiry dates
            expire_matrix["LastTrade"] = pd.to_datetime(expire_matrix["LastTrade"])
            rows_to_drop = 5
            today = pd.Timestamp.today()
            
            expire_matrix["Year"] = expire_matrix["LastTrade"].dt.year
            year_to_last_trade = expire_matrix.set_index("Year")["LastTrade"].to_dict()
            
            filtered_spread_dict = {}
            
            for year_key, df in spread_dict.items():
                df_copy = df.copy()  # Make a copy to avoid modifying original
                
                if year_key in year_to_last_trade:
                    last_trade_date = year_to_last_trade[year_key]
                    df_copy = df_copy[df_copy.index <= last_trade_date]
                    
                    if last_trade_date < today and len(df_copy) > rows_to_drop:
                        df_copy = df_copy.iloc[:-rows_to_drop]
                    
                    df_copy['LastTrade'] = last_trade_date
                else:
                    # If no expire date match, use current date as LastTrade (as a fallback)
                    df_copy['LastTrade'] = pd.Timestamp.today()
                
                filtered_spread_dict[year_key] = df_copy
        
        print(f"DEBUG: Filtered spread dict has {len(filtered_spread_dict)} years")
        
        # Prepare final DataFrame
        combined_spread_list = []
        
        for year, df in filtered_spread_dict.items():
            if not df.empty and 'spread' in df.columns:
                df_copy = df[['spread', 'LastTrade']].copy()
                df_copy["Year"] = str(year)
                df_copy["Date"] = df_copy.index
                combined_spread_list.append(df_copy.reset_index(drop=True))
                print(f"DEBUG: Added {len(df_copy)} rows for year {year}")
        
        print(f"DEBUG: Combined spread list has {len(combined_spread_list)} dataframes")
        
        if not combined_spread_list:
            debug_info = [
                f"Prices dict keys: {list(prices_dict.keys())}",
                f"Expire list: {expire_list}",
                f"Combined list: {combined_list}",
                f"Expire matrix matches: {len(expire_matrix)}",
                f"Spread dict years: {list(spread_dict.keys())}",
                f"Filtered spread dict years: {list(filtered_spread_dict.keys())}"
            ]
            return {}, dbc.Alert([
                html.H5("❌ No spread data generated!"),
                html.P("Debug information:"),
                html.Ul([html.Li(info) for info in debug_info])
            ], color="warning")
        
        final_spread_df = pd.concat(combined_spread_list, ignore_index=True)
        final_spread_df = final_spread_df[['Date', 'Year', 'spread', 'LastTrade']]
        
        # Add metadata
        final_spread_df['InstrumentName'] = name
        final_spread_df['Group'] = group
        final_spread_df['Region'] = region
        final_spread_df['Month'] = month
        final_spread_df['RollFlag'] = roll_flag
        final_spread_df['Desc'] = desc
        
        # Convert to JSON for storage
        data_json = final_spread_df.to_json(date_format='iso', orient='records')
        
        success_msg = dbc.Alert([
            html.H5("✅ Calculation Successful!", className="alert-heading"),
            html.P(f"Generated spread data for {len(final_spread_df)} data points across {len(filtered_spread_dict)} contract years.")
        ], color="success")
        
        return data_json, success_msg
        
    except Exception as e:
        import traceback
        error_msg = dbc.Alert([
            html.H5("❌ Calculation Failed!", className="alert-heading"),
            html.P(f"Error: {str(e)}"),
            html.Hr(),
            html.P("Full traceback:", className="fw-bold"),
            html.Pre(traceback.format_exc(), style={'font-size': '10px', 'white-space': 'pre-wrap'})
        ], color="danger")
        return {}, error_msg

@app.callback(
    Output('spread-figure', 'figure'),
    Output('spread-histogram', 'figure'),
    Input('calculated-data', 'data')
)
def update_figures(data_json):
    if not data_json:
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="No data to display",
            template='plotly_dark'
        )
        return empty_fig, empty_fig
    
    # Load data from JSON using StringIO to handle deprecation warning
    filtered_df = pd.read_json(StringIO(data_json), orient='records')
    filtered_df["Date"] = pd.to_datetime(filtered_df["Date"])
    filtered_df["LastTrade"] = pd.to_datetime(filtered_df["LastTrade"])
    
    # Sort data by date
    filtered_df = filtered_df.sort_values("Date")
    today = pd.Timestamp.today().normalize()

    # Separate historical and current data
    # Historical data: LastTrade is today or in the past
    # Current data: LastTrade is in the future
    historical_df = filtered_df[filtered_df['LastTrade'] <= today].copy()
    current_df = filtered_df[filtered_df['LastTrade'] > today].copy()
    
    fig = go.Figure()
    seasonal_data = {}

    # Process historical data
    for year in sorted(historical_df['Year'].unique()): # Sort years for consistent legend order
        year_group = historical_df[historical_df['Year'] == year].copy()
        
        # Use the actual LastTrade date for filtering if available, otherwise just use all data for that year
        # This is where the divergence from dash_app_dev_v102.py might occur if your LastTrade dates
        # from expire_matrix are not aligned for historical data.
        # To strictly replicate dash_app_dev_v102.py, we need 252 days *after* filtering.
        last_trade_for_year = year_group['LastTrade'].max()
        year_filtered = year_group[year_group['Date'] <= last_trade_for_year].sort_values('Date').tail(252).copy()
        
        # Crucial check: only include years with exactly 252 trading days for the seasonal plot
        if len(year_filtered) == 252:
            year_filtered = year_filtered.reset_index(drop=True)
            year_filtered['TradingDay'] = range(1, 253) # Consistent 1 to 252
            seasonal_data[str(year)] = year_filtered
            print(f"DEBUG: Added {len(year_filtered)} data points for historical year {year}")
        else:
            print(f"WARNING: Historical year {year} does not have 252 trading days after filtering by LastTrade ({len(year_filtered)}). Skipping for seasonal plot to match `dash_app_dev_v102.py` behavior.")

    # Process current data
    if not current_df.empty:
        # Find the max LastTrade from historical data to determine the start of the "current" season
        # If no historical data, assume current starts from the earliest date in current_df
        last_hist_trade_date = historical_df['LastTrade'].max() if not historical_df.empty else None

        # Determine the start date for the "Current" spread.
        # This needs to align with how `dash_app_dev_v102.py` selects its "Current" data.
        # `dash_app_dev_v102.py` uses `next_month_start = (last_hist_trade + pd.offsets.MonthBegin(1)).normalize()`
        # If there's no historical data, we need a fallback for `next_month_start`.
        if last_hist_trade_date is not None and not pd.isna(last_hist_trade_date):
            next_month_start = (last_hist_trade_date + pd.offsets.MonthBegin(1)).normalize()
        else:
            # Fallback: if no historical data, start current from the earliest date in the current_df
            next_month_start = current_df['Date'].min().normalize()

        current_filtered = current_df[current_df['Date'] >= next_month_start].sort_values('Date').head(252).copy()

        if not current_filtered.empty:
            current_filtered = current_filtered.reset_index(drop=True)
            current_filtered['TradingDay'] = range(1, len(current_filtered) + 1)
            seasonal_data["Current"] = current_filtered
            print(f"DEBUG: Added {len(current_filtered)} data points for Current season.")
        else:
            print("WARNING: No data for Current season after filtering.")
    else:
        print("INFO: No current data available (LastTrade > today).")

    print(f"DEBUG: Seasonal data keys after processing: {list(seasonal_data.keys())}")
    
    # Add traces to figure
    if not seasonal_data:
        print("DEBUG: No seasonal data (no years with 252 days or valid current data). Showing all data as simple time series.")
        # Fallback to simple time series if no seasonal data can be plotted
        fig.add_trace(go.Scatter(
            x=filtered_df["Date"],
            y=filtered_df["spread"],
            mode="lines",
            name="All Data (Time Series)",
            line=dict(color="lightblue", width=2)
        ))
        fig.update_layout(
            title="Spread Time Series (No Seasonal Data Available)",
            xaxis_title="Date",
            yaxis_title="Spread",
            margin=dict(l=40, r=40, t=60, b=40),
            template='plotly_dark'
        )
    else:
        for label, df in seasonal_data.items():
            fig.add_trace(go.Scatter(
                x=df["TradingDay"],
                y=df["spread"],
                mode="lines",
                name=label,
                line=dict(color="white" if label == "Current" else None,
                          width=3 if label == "Current" else 1.5),
                opacity=1.0 if label == "Current" else 0.6
            ))

        fig.update_layout(
            title="Seasonal Spread by Year",
            xaxis_title="Trading Day (1 to 252)",
            yaxis_title="Spread",
            margin=dict(l=40, r=40, t=60, b=40),
            legend_title="Season",
            template='plotly_dark'
        )

    # Create histogram
    hist_fig = go.Figure()
    if not filtered_df.empty and 'spread' in filtered_df.columns:
        hist_fig.add_trace(go.Histogram(
            x=filtered_df["spread"],
            marker_color='lightblue',
            nbinsx=50
        ))

    hist_fig.update_layout(
        title="Distribution of Spread (Histogram)",
        xaxis_title="Spread",
        yaxis_title="Frequency",
        template="plotly_dark",
        margin=dict(l=40, r=40, t=60, b=40)
    )

    print(f"DEBUG: Histogram data points: {len(filtered_df)}")
    if not filtered_df.empty:
        print(f"DEBUG: Spread range: {filtered_df['spread'].min():.2f} to {filtered_df['spread'].max():.2f}")

    return fig, hist_fig

@app.callback(
    Output('data-preview', 'data'),
    Output('data-preview', 'columns'),
    Input('calculated-data', 'data')
)
def update_table(data_json):
    if not data_json:
        return [], []
    
    # Load data from JSON using StringIO to handle deprecation warning
    data = pd.read_json(StringIO(data_json), orient='records')
    data["Date"] = pd.to_datetime(data["Date"])
    data["LastTrade"] = pd.to_datetime(data["LastTrade"])
    data = data.sort_values("Date")
    
    columns = [{"name": i, "id": i} for i in data.columns]
    return data.to_dict("records"), columns

if __name__ == '__main__':
    app.run(port=8051)