import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import dash_table
from datetime import datetime, timedelta
import ast
from sqlalchemy import create_engine
from urllib import parse
import sys
import calendar

# --- Start of seasonalFunctions.py content (modified for direct use) ---
# Note: GvWSConnection and gcc_sparta_library are assumed to be available
# in the environment or need to be provided. For this standalone script,
# a placeholder for get_mv_data is used if the actual library isn't present.

try:
    from gcc_sparta_library import get_mv_data
except ImportError:
    print("Warning: 'gcc_sparta_library' not found. Using a dummy get_mv_data function.")
    # Dummy function for demonstration if gcc_sparta_library is not available
    def get_mv_data(symbol, data_type, start_date, end_date):
        print(f"Dummy get_mv_data called for {symbol} from {start_date} to {end_date}")
        # Return a dummy DataFrame for testing purposes
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        if len(dates) == 0:
            return pd.DataFrame()
        dummy_data = {
            'date': dates,
            'close': [100 + i * 0.5 + (i % 10) * 2 for i in range(len(dates))],
            'open': [99 + i * 0.5 for i in range(len(dates))],
            'high': [101 + i * 0.5 + 5 for i in range(len(dates))],
            'low': [98 + i * 0.5 - 5 for i in range(len(dates))],
            'volume': [1000 + i * 10 for i in range(len(dates))]
        }
        return 0


def generateYearList(contractMonthsList, yearOffsetList):
    if len(contractMonthsList) != len(yearOffsetList):
        raise ValueError("contractMonthsList and yearOffsetList must be the same length.")

    if any(offset < 0 for offset in yearOffsetList):
        raise ValueError("yearOffsetList cannot contain negative values.")

    current_year = datetime.today().year
    current_month = datetime.today().month

    year_list = []
    futuresContractDict= {'F':{'abr':'Jan','num':1},'G':{'abr':'Feb','num':2},'H':{'abr':'Mar','num':3},'J':{'abr':'Apr','num':4},
                          'K':{'abr':'May','num':5},'M':{'abr':'Jun','num':6},'N':{'abr':'Jul','num':7},'Q':{'abr':'Aug','num':8},
                          'U':{'abr':'Sep','num':9},'V':{'abr':'Oct','num':10},'X':{'abr':'Nov','num':11},'Z':{'abr':'Dec','num':12}}

    for i, contract_month_code in enumerate(contractMonthsList):
        offset = yearOffsetList[i]
        target_year = current_year + offset

        contract_month_num = None
        for key, value in futuresContractDict.items():
            if key == contract_month_code:
                contract_month_num = value['num']
                break
        
        if contract_month_num is None:
            raise ValueError(f"Invalid contract month code: {contract_month_code}")

        if contract_month_num <= current_month:
            target_year += 1
        
        year_list.append(str(target_year)[-2:])

    return year_list


def generate_contract_data_sparta(tickerList, contractMonthsList, yearList, weightsList, convList, yearsBack):
    """
    Generates contract data for a list of tickers, fetching daily prices using get_mv_data.
    Retries fetching up to 3 times if data is not returned.

    :param tickerList: List of ticker symbols (e.g., ['SPX', 'NDX']).
    :param contractMonthsList: List of contract months corresponding to each ticker.
    :param yearList: List of starting years for contracts corresponding to each ticker.
    :param weightsList: List of weights corresponding to each ticker.
    :param convList: List of conversion factors corresponding to each ticker.
    :param yearsBack: Number of years to go back for contract data.
    :return: A tuple containing:
             - contract_data (dict): A dictionary where keys are tickers and values are
                                     dictionaries containing 'Prices df', 'ContractList',
                                     'Weights', and 'Conversion'.
             - expireList (list): A list of the last 3 characters of each contract from the first ticker.
    """
    contract_data = {}
    expireList = None

    past_date = datetime.today().replace(year=datetime.today().year - (yearsBack + 2))
    start_date_obj = past_date
    end_date_obj = datetime.now()

    for i, t in enumerate(tickerList): # Changed to tickerList
        contractMonth = contractMonthsList[i]
        startYear = int(yearList[i])
        contractList = [f"{t}{contractMonth}{str(startYear - y).zfill(2)}" for y in range(yearsBack)]

        all_contract_dfs = []
        for contract_symbol in contractList:
            success = False
            for attempt in range(3):
                try:
                    contract_df = get_mv_data(
                        symbol=contract_symbol,
                        data_type='daily',
                        start_date=start_date_obj,
                        end_date=end_date_obj
                    )

                    if contract_df is not None and not contract_df.empty:
                        contract_df = contract_df.copy()
                        contract_df['symbol'] = contract_symbol
                        all_contract_dfs.append(contract_df)
                        print(f"Successfully retrieved daily data for contract: {contract_symbol}")
                        success = True
                        break
                    else:
                        print(f"Attempt {attempt + 1}: Empty DataFrame for {contract_symbol}")
                except Exception as e:
                    print(f"Attempt {attempt + 1}: Error retrieving {contract_symbol}: {e}")
                import time
                time.sleep(1)

            if not success:
                print(f"Failed to retrieve data for {contract_symbol} after 3 attempts.")

        if not all_contract_dfs:
            print(f"No daily data retrieved for any contracts of ticker {t}. Skipping this ticker.")
            continue

        df = pd.concat(all_contract_dfs, ignore_index=True)
        df.columns = [col.lower() for col in df.columns]
        df.rename(columns={'date': 'Date', 'close': 'close'}, inplace=True)

        required_cols = ['symbol', 'Date', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns {missing_cols} in DataFrame for {t}. Skipping this ticker.")
            continue

        df['WeightedPrice'] = df['close'] * convList[i] * weightsList[i] # Use lists here

        contract_data[t] = {
            'Prices df': df,
            "ContractList": contractList,
            "Weights": weightsList[i],
            "Conversion": convList[i]
        }

        if i == 0:
            expireList = [c[-3:] for c in contractList]

    return contract_data, expireList


def validate_contract_data(contract_data):
    contract_lengths = {ticker: len(data['ContractList']) for ticker, data in contract_data.items()}

    unique_lengths = set(contract_lengths.values())
    if len(unique_lengths) == 1:
        print(f"\u2705 All ContractList lengths are equal: {unique_lengths.pop()}")
    else:
        print("\u274C ContractList lengths are not equal!")
        for ticker, length in contract_lengths.items():
            print(f"{ticker}: {length} contracts")

    missing_data = []
    for ticker, data in contract_data.items():
        if 'Weights' not in data or 'Conversion' not in data:
            missing_data.append(ticker)

    if missing_data:
        print("\u274C The following tickers are missing Weights or Conversion:")
        for ticker in missing_data:
            print(f"{ticker}: Missing {['Weights' if 'Weights' not in contract_data[ticker] else 'Conversion'][0]}")
    else:
        print("\u2705 All tickers have Weights and Conversion.")

# --- End of seasonalFunctions.py content ---


# SQL Connection (kept for completeness, but not used for data loading in this app)
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

# Futures contract dictionary (from PriceBuilding_v101.py)
futuresContractDict= {'F':{'abr':'Jan','num':1},'G':{'abr':'Feb','num':2},'H':{'abr':'Mar','num':3},'J':{'abr':'Apr','num':4},
                      'K':{'abr':'May','num':5},'M':{'abr':'Jun','num':6},'N':{'abr':'Jul','num':7},'Q':{'abr':'Aug','num':8},
                      'U':{'abr':'Sep','num':9},'V':{'abr':'Oct','num':10},'X':{'abr':'Nov','num':11},'Z':{'abr':'Dec','num':12}}

# Initialize Dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    html.H2("Seasonal Spread Analysis (On-the-Fly)", className="my-4 text-center"),

    dbc.Card(
        dbc.CardBody([
            html.H4("Input Parameters", className="card-title"),
            dbc.Row([
                dbc.Col(dbc.Label("Name:")),
                dbc.Col(dcc.Input(id='input-name', type='text', value='NWE HSFO - NWE Naphtha', className="mb-2")),
                dbc.Col(dbc.Label("Ticker List (e.g., ['#BRGBM','ICENBAM']):")),
                dbc.Col(dcc.Input(id='input-tickerlist', type='text', value="['#BRGBM','#ICENBAM']", className="mb-2")),
            ]),
            dbc.Row([
                dbc.Col(dbc.Label("Contract Months (e.g., ['V','V']):")),
                dbc.Col(dcc.Input(id='input-contractmonths', type='text', value="['V','V']", className="mb-2")),
                dbc.Col(dbc.Label("Year Offset (e.g., [0, 0]):")),
                dbc.Col(dcc.Input(id='input-yearoffset', type='text', value="[0, 0]", className="mb-2")),
            ]),
            dbc.Row([
                dbc.Col(dbc.Label("Weights (e.g., [1,-1]):")),
                dbc.Col(dcc.Input(id='input-weights', type='text', value="[1,-1]", className="mb-2")),
                dbc.Col(dbc.Label("Conversion (e.g., [0.15748,1]):")),
                dbc.Col(dcc.Input(id='input-conv', type='text', value="[0.15748,1]", className="mb-2")),
            ]),
            dbc.Row([
                dbc.Col(dbc.Label("Roll Flag (e.g., HO):")),
                dbc.Col(dcc.Input(id='input-rollflag', type='text', value="HO", className="mb-2")),
                dbc.Col(dbc.Label("Month (e.g., V):")),
                dbc.Col(dcc.Input(id='input-month', type='text', value="V", className="mb-2")),
            ]),
            dbc.Row([
                dbc.Col(dbc.Label("Description (e.g., 100%NWE HSFO - 100% NWE Nap):")),
                dbc.Col(dcc.Input(id='input-desc', type='text', value="100%NWE HSFO - 100% NWE Nap", className="mb-2")),
                dbc.Col(dbc.Label("Group (e.g., HeavyDistillates):")),
                dbc.Col(dcc.Input(id='input-group', type='text', value="HeavyDistillates", className="mb-2")),
            ]),
            dbc.Row([
                dbc.Col(dbc.Label("Region (e.g., NWE):")),
                dbc.Col(dcc.Input(id='input-region', type='text', value="NWE", className="mb-2")),
                dbc.Col(dbc.Label("Years Back (e.g., 10):")),
                dbc.Col(dcc.Input(id='input-yearsback', type='number', value=10, className="mb-2")),
            ]),
            dbc.Button("Generate Plots", id='generate-button', color="primary", className="mt-3"),
        ]),
        className="mb-4"
    ),

    html.Div(id='output-container'), # Container for plots and table

], fluid=True, className="p-4")


@app.callback(
    Output('output-container', 'children'),
    Input('generate-button', 'n_clicks'),
    State('input-name', 'value'),
    State('input-tickerlist', 'value'),
    State('input-contractmonths', 'value'),
    State('input-yearoffset', 'value'),
    State('input-weights', 'value'),
    State('input-conv', 'value'),
    State('input-rollflag', 'value'),
    State('input-month', 'value'),
    State('input-desc', 'value'),
    State('input-group', 'value'),
    State('input-region', 'value'),
    State('input-yearsback', 'value'),
    prevent_initial_call=True
)
def update_output(n_clicks, name, ticker_list_str, contract_months_str, year_offset_str,
                  weights_str, conv_str, roll_flag, month, desc, group, region, years_back):
    if n_clicks is None:
        return html.Div()

    try:
        # Parse string inputs to Python lists
        tickerList = ast.literal_eval(ticker_list_str)
        contractMonthsList = ast.literal_eval(contract_months_str)
        yearOffsetList = ast.literal_eval(year_offset_str)
        weightsList = ast.literal_eval(weights_str)
        convList = ast.literal_eval(conv_str)

        variables = {
            'Name': name,
            'tickerList': tickerList,
            'contractMonthsList': contractMonthsList,
            'yearOffsetList': yearOffsetList,
            'weightsList': weightsList,
            'convList': convList,
            'rollFlag': roll_flag,
            'months': month,
            'desc': desc,
            'group': group,
            'region': region,
            'yearsBack': years_back
        }

        # --- Data Engineering Logic from PriceBuilding_v101.py ---
        yearList = generateYearList(variables['contractMonthsList'], variables['yearOffsetList'])
        
        # Use generate_contract_data_sparta
        pricesDict, expireList = generate_contract_data_sparta(
            variables['tickerList'], variables['contractMonthsList'], yearList,
            variables['weightsList'], variables['convList'], variables['yearsBack']
        )
        validate_contract_data(pricesDict)
        
        # Dummy expire matrix for on-the-fly calculation (not using SQL table)
        # In a real scenario, you might need a way to get this data without SQL.
        # For now, we'll construct a minimal one based on the generated contracts.
        expire_data = []
        if expireList:
            for i, contract_suffix in enumerate(expireList):
                # Extract MonthCode and Year from suffix (e.g., 'V25' -> 'V', '25')
                month_code = contract_suffix[0]
                year_suffix = contract_suffix[1:]
                full_year = 2000 + int(year_suffix) if int(year_suffix) < 50 else 1900 + int(year_suffix)
                
                # Find the last day of the month for the contract's expiry
                # This is a simplification; actual last trade dates would come from a real expire matrix.
                last_day_of_month = calendar.monthrange(full_year, futuresContractDict[month_code]['num'])[1]
                last_trade_date = datetime(full_year, futuresContractDict[month_code]['num'], last_day_of_month)

                # Assuming Ticker is the first ticker in the list for this example
                ticker_prefix = variables['tickerList'][0]
                expire_data.append({
                    'Ticker': ticker_prefix,
                    'MonthCode': month_code,
                    'LastTrade': last_trade_date.strftime('%Y-%m-%d'), # Format as string for consistency
                    'TickerMonthYear': f"{ticker_prefix}{month_code}{year_suffix}"
                })
        
        expireMatrix = pd.DataFrame(expire_data)
        expireMatrix["LastTrade"] = pd.to_datetime(expireMatrix["LastTrade"])
        expireMatrix["Year"] = expireMatrix["LastTrade"].dt.year
        year_to_last_trade = expireMatrix.set_index("Year")["LastTrade"].to_dict()

        spread_dict = {}
        if pricesDict:
            # Assume all product lists are same length
            # This needs to handle cases where pricesDict might be empty or have varying lengths
            # if not pricesDict or not next(iter(pricesDict.values()))["ContractList"]:
            #     raise ValueError("No contract data generated.")
            
            # Find the maximum number of contracts across all tickers
            num_contracts = 0
            for ticker_key, data in pricesDict.items():
                if data and "ContractList" in data:
                    num_contracts = max(num_contracts, len(data["ContractList"]))

            if num_contracts == 0:
                return html.Div(dbc.Alert("No contract data available to calculate spreads.", color="warning"))

            for i in range(num_contracts):
                combined_df = pd.DataFrame()
                first_contracts = []

                for ticker, data in pricesDict.items():
                    if i < len(data["ContractList"]):
                        first_contract = data["ContractList"][i]
                        first_contracts.append(first_contract)

                        temp_df = data["Prices df"][data["Prices df"]['symbol'] == first_contract][["Date", "WeightedPrice"]].copy()
                        temp_df["Date"] = pd.to_datetime(temp_df["Date"])
                        temp_df.set_index("Date", inplace=True)
                        temp_df.rename(columns={"WeightedPrice": first_contract}, inplace=True)

                        if combined_df.empty:
                            combined_df = temp_df
                        else:
                            combined_df = combined_df.join(temp_df, how="outer")

                if not combined_df.empty:
                    combined_df.dropna(inplace=True)
                    if not combined_df.empty:
                        combined_df["spread"] = combined_df.sum(axis=1, skipna=True)

                        year_suffix = first_contracts[0][-2:]
                        spread_year = 2000 + int(year_suffix) if int(year_suffix) < 50 else 1900 + int(year_suffix)
                        spread_dict[spread_year] = combined_df

        filtered_spread_dict = {}
        today = pd.Timestamp.today().normalize()

        for year_key, df in spread_dict.items():
            if year_key in year_to_last_trade:
                last_trade_date = year_to_last_trade[year_key]
                df['LastTrade'] = last_trade_date
                filtered_spread_dict[year_key] = df

        combined_spread_list = []
        for year, df in filtered_spread_dict.items():
            if not df.empty and 'spread' in df.columns:
                first__contract_col = df.columns[0]
                #GroupYear = 2000 + int(first__contract_col[-2:])
                #df_copy = df[['spread', 'LastTrade','GroupYear']].copy()
                df_copy = df[['spread', 'LastTrade']].copy()
                df_copy["Year"] = str(year)
                df_copy["Date"] = df_copy.index
                combined_spread_list.append(df_copy.reset_index(drop=True))

        if not combined_spread_list:
            return html.Div(dbc.Alert("No spread data could be generated with the provided inputs.", color="warning"))

        final_spread_df = pd.concat(combined_spread_list, ignore_index=True)
        final_spread_df = final_spread_df[['Date', 'Year', 'spread', 'LastTrade']]#,'GroupYear']]
        final_spread_df['InstrumentName'] = variables['Name']
        final_spread_df['Group'] = variables['group']
        final_spread_df['Region'] = variables['region']
        final_spread_df['Month'] = variables['months']
        final_spread_df['RollFlag'] = variables['rollFlag']
        final_spread_df['Desc'] = variables['desc']

        data = final_spread_df.copy() # Use this as the data for plotting

        # --- Plotting Logic from dash_preset.py ---
        filtered_df = data.copy()
        filtered_df = filtered_df.sort_values("Date")
        today = pd.Timestamp.today().normalize()

        historical_df = filtered_df[filtered_df['LastTrade'] <= today].copy()
        current_df = filtered_df[filtered_df['LastTrade'] > today].copy()

        fig = go.Figure()
        seasonal_data = {}

        for year in historical_df['Year'].unique():
            year_group = historical_df[historical_df['Year'] == year].copy()
            last_trade = year_group['LastTrade'].max()
            year_filtered = year_group[year_group['Date'] <= last_trade].sort_values('Date').tail(252).copy()
            if len(year_filtered) == 252:
                year_filtered = year_filtered.reset_index(drop=True)
                year_filtered['TradingDay'] = range(1, 253)
                seasonal_data[str(year)] = year_filtered

        if not historical_df.empty and not current_df.empty:
            last_hist_trade = historical_df['LastTrade'].max()
            # Ensure next_month_start is a Timestamp before adding MonthBegin
            if isinstance(last_hist_trade, pd.Timestamp):
                next_month_start = (last_hist_trade + pd.offsets.MonthBegin(1)).normalize()
            else: # Fallback if last_hist_trade is not a Timestamp
                next_month_start = pd.Timestamp(datetime.today().replace(day=1) + timedelta(days=32)).normalize() # Approx next month start

            current_filtered = current_df[current_df['Date'] >= next_month_start].sort_values('Date').head(252).copy()
            if not current_filtered.empty:
                current_filtered = current_filtered.reset_index(drop=True)
                current_filtered['TradingDay'] = range(1, len(current_filtered) + 1)
                seasonal_data["Current"] = current_filtered

        if not seasonal_data:
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

        hist_fig = go.Figure()
        if not filtered_df.empty and 'spread' in filtered_df.columns:
            spread_values = filtered_df["spread"]
            
            latest_spread = spread_values.iloc[-1] if not spread_values.empty else None
            mean_spread = spread_values.mean()
            median_spread = spread_values.median()
            std_dev = spread_values.std()

            hist_fig.add_trace(go.Histogram(
                x=spread_values,
                marker_color='lightblue',
                nbinsx=50,
                name='Spread Distribution'
            ))

            if latest_spread is not None:
                hist_fig.add_vline(x=latest_spread, line_dash="dash", line_color="yellow",
                                   annotation_text=f"Latest: {latest_spread:.2f}",
                                   annotation_position="top right", annotation_font_color="yellow")
            
            hist_fig.add_vline(x=mean_spread, line_dash="dash", line_color="red",
                               annotation_text=f"Mean: {mean_spread:.2f}",
                               annotation_position="top left", annotation_font_color="red")
            
            hist_fig.add_vline(x=median_spread, line_dash="dash", line_color="green",
                               annotation_text=f"Median: {median_spread:.2f}",
                               annotation_position="top right", annotation_font_color="green")
            
            hist_fig.add_vline(x=mean_spread - std_dev, line_dash="dot", line_color="orange",
                               annotation_text=f"-1 Std Dev: {(mean_spread - std_dev):.2f}",
                               annotation_position="bottom left", annotation_font_color="orange")
            hist_fig.add_vline(x=mean_spread + std_dev, line_dash="dot", line_color="orange",
                               annotation_text=f"+1 Std Dev: {(mean_spread + std_dev):.2f}",
                               annotation_position="bottom right", annotation_font_color="orange")
                               
            hist_fig.add_vline(x=mean_spread - 2 * std_dev, line_dash="dot", line_color="purple",
                               annotation_text=f"-2 Std Dev: {(mean_spread - 2 * std_dev):.2f}",
                               annotation_position="bottom left", annotation_font_color="purple")
            hist_fig.add_vline(x=mean_spread + 2 * std_dev, line_dash="dot", line_color="purple",
                               annotation_text=f"+2 Std Dev: {(mean_spread + 2 * std_dev):.2f}",
                               annotation_position="bottom right", annotation_font_color="purple")

            stats_text = (
                f"Latest Spread: {latest_spread:.2f}<br>"
                f"Mean: {mean_spread:.2f}<br>"
                f"Median: {median_spread:.2f}<br>"
                f"Std Dev: {std_dev:.2f}"
            )
            
            hist_fig.add_annotation(
                text=stats_text,
                xref="paper", yref="paper",
                x=0.98, y=0.98,
                showarrow=False,
                align="left",
                bordercolor="white",
                borderwidth=1,
                bgcolor="rgba(0,0,0,0.7)",
                font=dict(color="white", size=10)
            )

        hist_fig.update_layout(
            title="Distribution of Spread (Histogram) with Key Statistics",
            xaxis_title="Spread",
            yaxis_title="Frequency",
            template="plotly_dark",
            margin=dict(l=40, r=40, t=60, b=40)
        )

        # DataTable: Filtered Data Preview
        filtered_df_table = data.copy()
        filtered_df_table["LastTrade"] = pd.to_datetime(filtered_df_table["LastTrade"], errors="coerce")
        filtered_df_table["Date"] = pd.to_datetime(filtered_df_table["Date"], errors="coerce")
        filtered_df_table["Year"] = filtered_df_table["Date"].dt.year

        if filtered_df_table.empty:
            table_data = []
            table_columns = []
        else:
            table_columns = [{"name": i, "id": i} for i in filtered_df_table.columns]
            table_data = filtered_df_table.to_dict("records")

        return html.Div([
            html.Br(),
            dcc.Graph(id='spread-figure', figure=fig),
            html.Br(),
            dcc.Graph(id='spread-histogram', figure=hist_fig),
            html.H4("Generated Data Preview", className="mt-4"),
            dash_table.DataTable(
                id='data-preview',
                data=table_data,
                columns=table_columns,
                page_size=10,
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
            )
        ])

    except Exception as e:
        return html.Div(dbc.Alert(f"Error processing input or generating data: {e}", color="danger"))


if __name__ == '__main__':
    app.run(debug=True, port=8051)
