from GvWSConnection import *
from datetime import datetime as dt
import pandas as pd 
from datetime import timedelta, datetime as dt
from dotenv import load_dotenv
from gcc_sparta_library import get_mv_data
import os 
import plotly.graph_objects as go
# Load environment variables from .env file
load_dotenv('credential.env')

GvWSUSERNAME = os.getenv("GvWSUSERNAME"),
GvWSPASSWORD = os.getenv("GvWSPASSWORD"),

conn = GvWSConnection(GvWSUSERNAME, GvWSPASSWORD)

def generateYearList(contractMonthsList, yearOffsetList):
    if len(contractMonthsList) != len(yearOffsetList):
        raise ValueError("contractMonthsList and yearOffsetList must be the same length.")
    
    if any(offset < 0 for offset in yearOffsetList):
        raise ValueError("yearOffsetList cannot contain negative values.")

    current_year = dt.today().year
    year_list = [str(current_year + offset)[-2:] for offset in yearOffsetList]
    
    return year_list


def contractMonths(expireIn,contractRollIn,ContractMonthIn):
    
    tempExpire = expireIn[expireIn['Ticker']==contractRollIn]
    
    tempExpire.set_index(pd.to_datetime(tempExpire['LastTrade'], format='%m/%d/%y'), inplace = True)
    filtered_contracts = tempExpire[tempExpire.index > dt.today()].copy()
    
    expireDate = filtered_contracts[filtered_contracts['MonthCode'] == ContractMonthIn]
    
    return expireDate.iloc[0, :]



def generate_contract_data(ticker, contractMonthsList, yearList, weights, conv, yearsBack, conn):
    contract_data = {}

    # Calculate the past date (yearsBack + 2 years)
    past_date = dt.today().replace(year=dt.today().year - (yearsBack + 2))
    
    # Format it as 'MM/DD/YYYY'
    startDate = past_date.strftime('%m/%d/%Y')

    expireList = None  # Initialize expireList

    for i, t in enumerate(ticker):
        contractMonth = contractMonthsList[i]
        startYear = int(yearList[i])

        # Generate list of contracts going back 'yearsBack' years
        contractList = [f"{t}{contractMonth}{startYear - y}" for y in range(yearsBack)]
        
        # Retrieve daily prices
        d1 = conn.get_daily(contractList, start_date=dt.strptime(startDate, '%m/%d/%Y'))
        
        # Convert to DataFrame
        df = pd.DataFrame(d1)
        
        # Rename columns properly
        df.rename(columns={'pricesymbol': 'symbol', 'tradedatetimeutc': 'Date'}, inplace=True)
        
        # Select only the required columns
        df = df.loc[:, ['symbol', 'Date', 'close']]
        
        # Compute weighted price
        df['WeightedPrice'] = df['close'] * conv[i] * weights[i]
        
        # Store data in dictionary
        contract_data[t] = {
            'Prices df': df,  # DataFrame with original prices
            "ContractList": contractList,
            "Weights": weights[i],
            "Conversion": conv[i]
        }

        # If it's the first ticker, create expireList (last 3 characters of each contract)
        if i == 0:
            expireList = [c[-3:] for c in contractList]

    return contract_data, expireList


def generate_contract_data_sparta(ticker, contractMonthsList, yearList, weights, conv, yearsBack):
    """
    Generates contract data for a list of tickers, fetching daily prices using get_mv_data.
    Retries fetching up to 3 times if data is not returned.

    :param ticker: List of ticker symbols (e.g., ['SPX', 'NDX']).
    :param contractMonthsList: List of contract months corresponding to each ticker.
    :param yearList: List of starting years for contracts corresponding to each ticker.
    :param weights: List of weights corresponding to each ticker.
    :param conv: List of conversion factors corresponding to each ticker.
    :param yearsBack: Number of years to go back for contract data.
    :return: A tuple containing:
             - contract_data (dict): A dictionary where keys are tickers and values are
                                     dictionaries containing 'Prices df', 'ContractList',
                                     'Weights', and 'Conversion'.
             - expireList (list): A list of the last 3 characters of each contract from the first ticker.
    """
    contract_data = {}
    expireList = None

    past_date = dt.today().replace(year=dt.today().year - (yearsBack + 2))
    start_date_obj = past_date
    end_date_obj = dt.now()

    for i, t in enumerate(ticker):
        contractMonth = contractMonthsList[i]
        startYear = int(yearList[i])
        contractList = [f"{t}{contractMonth}{startYear - y}" for y in range(yearsBack)]

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
                        break  # exit retry loop
                    else:
                        print(f"Attempt {attempt + 1}: Empty DataFrame for {contract_symbol}")
                except Exception as e:
                    print(f"Attempt {attempt + 1}: Error retrieving {contract_symbol}: {e}")
                time.sleep(1)  # optional: wait between retries

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

        df['WeightedPrice'] = df['close'] * conv[i] * weights[i]

        contract_data[t] = {
            'Prices df': df,
            "ContractList": contractList,
            "Weights": weights[i],
            "Conversion": conv[i]
        }

        if i == 0:
            expireList = [c[-3:] for c in contractList]

    return contract_data, expireList


def validate_contract_data(contract_data):
    contract_lengths = {ticker: len(data['ContractList']) for ticker, data in contract_data.items()}
    
    # Check if all ContractList lengths are the same
    unique_lengths = set(contract_lengths.values())
    if len(unique_lengths) == 1:
        print(f"\u2705 All ContractList lengths are equal: {unique_lengths.pop()}")
    else:
        print("\u274C ContractList lengths are not equal!")
        for ticker, length in contract_lengths.items():
            print(f"{ticker}: {length} contracts")
    
    # Check if Weights and Conversion exist for each key
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

# def create_seasonal_spread_plot(filtered_df):
#     """
#     Creates a seasonal spread plot based on historical and current data.

#     Args:
#         filtered_df (pd.DataFrame): DataFrame containing 'Date', 'Year', 'spread', and 'LastTrade' columns.

#     Returns:
#         go.Figure: Plotly Figure object for the seasonal spread.
#     """
#     fig = go.Figure()
#     seasonal_data = {}
#     today = pd.Timestamp.today().normalize()

#     historical_df = filtered_df[filtered_df['LastTrade'] <= today].copy()
#     current_df = filtered_df[filtered_df['LastTrade'] > today].copy()

#     for year in sorted(historical_df['Year'].unique()):
#         year_group = historical_df[historical_df['Year'] == year].copy()
#         last_trade = year_group['LastTrade'].max()
#         year_filtered = year_group[year_group['Date'] <= last_trade].sort_values('Date').tail(252).copy()
#         if len(year_filtered) == 252:
#             year_filtered = year_filtered.reset_index(drop=True)
#             year_filtered['TradingDay'] = range(1, 253)
#             seasonal_data[str(year)] = year_filtered

#     if not current_df.empty:
#         last_hist_trade = historical_df['LastTrade'].max() if not historical_df.empty else None
#         if last_hist_trade is not None and not pd.isna(last_hist_trade):
#             next_month_start = (last_hist_trade + pd.offsets.MonthBegin(1)).normalize()
#         else:
#             next_month_start = current_df['Date'].min().normalize()

#         current_filtered = current_df[current_df['Date'] >= next_month_start].sort_values('Date').head(252).copy()
#         if not current_filtered.empty:
#             current_filtered = current_filtered.reset_index(drop=True)
#             current_filtered['TradingDay'] = range(1, len(current_filtered) + 1)
#             seasonal_data["Current"] = current_filtered

#     if not seasonal_data:
#         # Fallback for when no seasonal data can be plotted
#         fig.add_trace(go.Scatter(
#             x=filtered_df["Date"],
#             y=filtered_df["spread"],
#             mode="lines",
#             name="All Data (Time Series)",
#             line=dict(color="lightblue", width=2)
#         ))
#         fig.update_layout(
#             title="Spread Time Series (No Seasonal Data Available)",
#             xaxis_title="Date",
#             yaxis_title="Spread",
#             margin=dict(l=40, r=40, t=60, b=40),
#             template='plotly_dark'
#         )
#     else:
#         for label, df in seasonal_data.items():
#             fig.add_trace(go.Scatter(
#                 x=df["TradingDay"],
#                 y=df["spread"],
#                 mode="lines",
#                 name=label,
#                 line=dict(color="white" if label == "Current" else None,
#                          width=3 if label == "Current" else 1.5),
#                 opacity=1.0 if label == "Current" else 0.6
#             ))

#         fig.update_layout(
#             title="Seasonal Spread by Year",
#             xaxis_title="Trading Day (1 to 252)",
#             yaxis_title="Spread",
#             margin=dict(l=40, r=40, t=60, b=40),
#             legend_title="Season",
#             template='plotly_dark'
#         )
#     return fig

# def create_spread_histogram(filtered_df):
#     """
#     Creates a histogram of spread values with statistical markers.

#     Args:
#         filtered_df (pd.DataFrame): DataFrame containing 'spread' column.

#     Returns:
#         go.Figure: Plotly Figure object for the spread histogram.
#     """
#     hist_fig = go.Figure()
#     if not filtered_df.empty and 'spread' in filtered_df.columns:
#         spread_values = filtered_df["spread"].dropna()

#         if not spread_values.empty:
#             latest_spread = spread_values.iloc[-1]
#             mean_spread = spread_values.mean()
#             median_spread = spread_values.median()
#             std_dev = spread_values.std()
#             std_dev_1_plus = mean_spread + std_dev
#             std_dev_1_minus = mean_spread - std_dev
#             std_dev_2_plus = mean_spread + (2 * std_dev)
#             std_dev_2_minus = mean_spread - (2 * std_dev)

#             hist_fig.add_trace(go.Histogram(
#                 x=spread_values,
#                 marker_color='lightblue',
#                 nbinsx=50,
#                 name='Spread Distribution'
#             ))

#             if latest_spread is not None:
#                 hist_fig.add_vline(x=latest_spread, line_dash="dash", line_color="orange",
#                                   annotation_text=f"Latest Spread: {latest_spread:.2f}",
#                                   annotation_position="top right")
#             hist_fig.add_vline(x=mean_spread, line_dash="dash", line_color="red",
#                                 annotation_text=f"Mean: {mean_spread:.2f}",
#                                 annotation_position="top left")
#             hist_fig.add_vline(x=median_spread, line_dash="dash", line_color="purple",
#                                 annotation_text=f"Median: {median_spread:.2f}",
#                                 annotation_position="top")

#             hist_fig.add_vline(x=std_dev_1_plus, line_dash="dot", line_color="lightgreen",
#                                 annotation_text=f"+1 Std Dev: {std_dev_1_plus:.2f}",
#                                 annotation_position="bottom right")
#             hist_fig.add_vline(x=std_dev_1_minus, line_dash="dot", line_color="lightgreen",
#                                 annotation_text=f"-1 Std Dev: {std_dev_1_minus:.2f}",
#                                 annotation_position="bottom left")
#             hist_fig.add_vline(x=std_dev_2_plus, line_dash="dot", line_color="cyan",
#                                 annotation_text=f"+2 Std Dev: {std_dev_2_plus:.2f}",
#                                 annotation_position="bottom right")
#             hist_fig.add_vline(x=std_dev_2_minus, line_dash="dot", line_color="cyan",
#                                 annotation_text=f"-2 Std Dev: {std_dev_2_minus:.2f}",
#                                 annotation_position="bottom left")
#     else:
#         # If no data, return an empty histogram
#         hist_fig.update_layout(
#             title="No data to display",
#             template='plotly_dark'
#         )

#     hist_fig.update_layout(
#         title="Distribution of Spread (Histogram) with Statistics",
#         xaxis_title="Spread",
#         yaxis_title="Frequency",
#         template="plotly_dark",
#         margin=dict(l=40, r=40, t=60, b=40),
#         showlegend=False
#     )
#     return hist_fig