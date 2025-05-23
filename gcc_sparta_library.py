#gcc_sparta_library.py

import win32com.client
import pythoncom
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("credential.env")

def connect_to_mv_com_server():
    """Establish connection to MV COM server using credentials from .env file."""
    try:
        # Fetch credentials from environment variables
        server = os.getenv("USERNAME_LOGIN")
        password = os.getenv("PASSWORD_LOGIN")

        if not server or not password:
            raise ValueError("Credentials are missing in the environment file.")

        pythoncom.CoInitialize()
        con = win32com.client.Dispatch("Mv.Connectivity.ComClient.ServerConnection")
        con.Connect(server, password)
        return con
    except Exception as e:
        print(f"Error connecting to MV COM server: {e}")
        return None

def fetch_daily_data(con, symbol: str, start_date: datetime, end_date: datetime):
    """Fetch daily data for the given symbol and date range."""
    try:
        daily_data_raw = con.GetDailyRange(symbol=symbol, From=start_date, to=end_date)
        return list(daily_data_raw)
    except Exception as e:
        print(f"Error fetching daily data: {e}")
        return []

def fetch_option_chain_data(con, symbol: str, strike_num: int):
    """Fetch option chain data for the given symbol and number of strikes."""
    try:
        option_data_raw = con.GetOptionChain(symbol, strike_num)
        return list(option_data_raw)
    except Exception as e:
        print(f"Error fetching option chain data: {e}")
        return []

def inspect_com_object(obj, depth=0, max_depth=1):
    """
    Prints non-callable attributes of a COM object and
    recursively inspects contained COM objects up to max_depth.
    """
    if depth > max_depth:
        return

    indent = "  " * depth
    print(f"{indent}--- Inspecting COM Object: {obj} (Depth: {depth}) ---")
    for attr in dir(obj):
        if not attr.startswith('_'):
            try:
                value = getattr(obj, attr)
                if not callable(value):
                    print(f"{indent}  {attr}: {value}")
                    # Recursively inspect if it's another COM object
                    if "PyIDispatch" in str(type(value)) and depth < max_depth:
                        print(f"{indent}  (Entering {attr})")
                        inspect_com_object(value, depth + 1, max_depth)
                        print(f"{indent}  (Exiting {attr})")
                # else:
                #     # Optionally print callable attributes for completeness
                #     # print(f"{indent}  {attr}: <method>")
            except Exception as e:
                print(f"{indent}  {attr}: Could not retrieve ({e})")
    print(f"{indent}-----------------------------------")


def daily_data_to_dataframe(daily_data):
    """Converts list of daily COM data objects to a pandas DataFrame."""
    data = []
    for day in daily_data:
        try:
            row = {
                "Date": pd.to_datetime(getattr(day, "StringDateTime", None)),
                "Open": getattr(day, "Open", None),
                "High": getattr(day, "High", None),
                "Low": getattr(day, "Low", None),
                "Close": getattr(day, "Close", None),
                "Volume": getattr(day, "Volume", None),
                "OpenInterest": getattr(day, "OpenInterest", None),
            }
            data.append(row)
        except Exception as e:
            print(f"Error processing record: {e}")
    return pd.DataFrame(data)

def option_chain_to_dataframe(option_chain_data):
    """
    Converts list of option chain COM data objects to a pandas DataFrame,
    capturing all relevant fields from the provided image.
    """
    data = []
    
    # Define a set of attributes to try and extract from the OptionValue COM object
    # These are common attributes for both Call and Put objects in the option chain.
    option_value_attrs = [
        "PriceSymbol", "ImpVol", "TheoVal", "Delta", "Gamma", "Rho",
        "Theta", "Vega", "Last", "TradeTime", "Bid", "Ask",
        "OpenInterest", "Volume", "ContractDate", "ExpirationDate", "DTE" # Added DTE
    ]

    for i, opt_row in enumerate(option_chain_data):
        try:
            # Get common data for the strike row
            underlying_price = getattr(opt_row, "Price", None) # This is actually the Strike Price
            atm_index = getattr(opt_row, "AtmIndex", None)
            
            # Extract Call and Put COM objects
            call_data = getattr(opt_row, "Call", None)
            put_data = getattr(opt_row, "Put", None)

            # Optional: Inspect the first COM object to help debug and discover attributes
            # For general use, keep inspect_first in get_mv_data as False
            # if i == 0 and True: # Set to True for verbose inspection
            #     print("\n--- Inspecting Option Chain Row COM Object (first row) ---")
            #     inspect_com_object(opt_row, max_depth=2) # Inspect deeper to find nested objects like Call/Put

            # Initialize row with common data (Underlying_Price from GetQuote will be used)
            row = {
                "Strike": underlying_price, # 'Price' in OptionChainRow is the Strike
                "ATM_Index": atm_index
            }
            
            # Add call option data
            if call_data:
                for attr in option_value_attrs:
                    value = getattr(call_data, attr, None)
                    if attr == "DTE" and value is None: # Calculate DTE if not directly provided
                        # Assuming ExpirationDate is a valid attribute
                        exp_date_com = getattr(call_data, "ExpirationDate", None)
                        if exp_date_com:
                            exp_date = pd.to_datetime(exp_date_com)
                            value = (exp_date - datetime.now()).days
                    row[f"Call_{attr}"] = value
            else:
                # Add None for all call-specific columns if no call data
                for attr in option_value_attrs:
                    row[f"Call_{attr}"] = None

            # Add put option data
            if put_data:
                for attr in option_value_attrs:
                    value = getattr(put_data, attr, None)
                    if attr == "DTE" and value is None: # Calculate DTE if not directly provided
                        # Assuming ExpirationDate is a valid attribute
                        exp_date_com = getattr(put_data, "ExpirationDate", None)
                        if exp_date_com:
                            exp_date = pd.to_datetime(exp_date_com)
                            value = (exp_date - datetime.now()).days
                    row[f"Put_{attr}"] = value
            else:
                # Add None for all put-specific columns if no put data
                for attr in option_value_attrs:
                    row[f"Put_{attr}"] = None

            data.append(row)
        except Exception as e:
            print(f"Error processing option record (row {i}): {e}")
    
    df = pd.DataFrame(data)
    
    # Add 'Underlying_Price' column based on the image's layout.
    # This value typically comes from a separate quote fetch, not directly in each option chain row.
    # We will add a placeholder or rely on a separate `get_mv_quote` call in the Streamlit app.
    # For now, let's keep it as None in the DataFrame and assume it's filled externally.
    if 'Underlying_Price' not in df.columns:
        df.insert(0, 'Underlying_Price', None)

    # Format the dataframe to match desired output columns from the image
    if not df.empty:
        # Define columns in the order they appear in the image
        # Left side (Calls)
        call_display_cols = [
            "Call_Delta", "Call_Gamma", "Call_Theta", "Call_Vega", "Call_Rho", # Rho is listed last in the image for Greeks
            "Call_ThVal", "Call_ImpVol", "Call_Last", "Call_Bid", "Call_Ask",
            "Call_Volume", "Call_OpenInterest", "Call_TradeTime", "Call_PriceSymbol",
            "Call_ContractDate", "Call_ExpirationDate", "Call_DTE"
        ]
        
        # Center columns (Strike)
        center_display_cols = ["Strike"] # ATM_Index might be useful but not explicitly shown for each row in the image's center
        
        # Right side (Puts)
        put_display_cols = [
            "Put_Delta", "Put_Gamma", "Put_Theta", "Put_Vega", "Put_Rho", # Rho is listed last in the image for Greeks
            "Put_ThVal", "Put_ImpVol", "Put_Last", "Put_Bid", "Put_Ask",
            "Put_Volume", "Put_OpenInterest", "Put_TradeTime", "Put_PriceSymbol",
            "Put_ContractDate", "Put_ExpirationDate", "Put_DTE"
        ]
        
        # Filter for existing columns and combine in desired order
        final_cols_order = [col for col in call_display_cols if col in df.columns] + \
                           [col for col in center_display_cols if col in df.columns] + \
                           [col for col in put_display_cols if col in df.columns]
        
        # Add any columns that might be in the DataFrame but not in the defined display lists (e.g., ATM_Index)
        other_cols = [col for col in df.columns if col not in final_cols_order]
        final_cols_order.extend(other_cols) # Append them at the end

        df = df[final_cols_order]
    
    return df

def get_mv_data(symbol: str, data_type: str, start_date: datetime = None, end_date: datetime = None, strike_num: int = None, inspect_first: bool = False):
    """
    Safely retrieve and process MV data.
    data_type can be 'daily' or 'option_chain'.
    For 'daily', start_date and end_date are required.
    For 'option_chain', strike_num is required.
    inspect_first: If True, performs a verbose inspection of the first COM object.
    """
    con = connect_to_mv_com_server()
    if con is None:
        raise RuntimeError("Failed to connect to MV COM server.")

    data_raw = []
    try:
        if data_type == 'daily':
            if not start_date or not end_date:
                raise ValueError("start_date and end_date are required for 'daily' data_type.")
            data_raw = fetch_daily_data(con, symbol, start_date, end_date)
            
        elif data_type == 'option_chain':
            if strike_num is None:
                raise ValueError("strike_num is required for 'option_chain' data_type.")
            data_raw = fetch_option_chain_data(con, symbol, strike_num)
        else:
            raise ValueError("Invalid data_type. Must be 'daily' or 'option_chain'.")

    except Exception as e:
        raise RuntimeError(f"Failed to fetch {data_type} data: {e}")

    if not data_raw:
        raise ValueError(f"No {data_type} data returned. This could be due to an invalid symbol or temporary server issue.")

    if inspect_first and data_raw:
        try:
            # Inspect the main COM object returned by GetOptionChain or GetDailyRange
            print(f"\n--- Initial Inspection of {data_type} COM Object (first element) ---")
            inspect_com_object(data_raw[0], max_depth=2) # Inspect deeper for option chain
        except Exception as e:
            print(f"Warning: Could not perform initial COM object inspection: {e}")

    try:
        if data_type == 'daily':
            df = daily_data_to_dataframe(data_raw)
        elif data_type == 'option_chain':
            df = option_chain_to_dataframe(data_raw)
            # The Underlying_Price column in the option_chain_to_dataframe is a placeholder.
            # We can try to get the actual underlying price here using get_mv_quote.
            try:
                underlying_quote = get_mv_quote(symbol)
                if underlying_quote and 'Last' in underlying_quote:
                    df['Underlying_Price'] = underlying_quote['Last']
            except Exception as e:
                print(f"Warning: Could not fetch underlying quote for {symbol}: {e}. 'Underlying_Price' will be None.")

        return df
    except Exception as e:
        raise RuntimeError(f"Failed to convert {data_type} data to DataFrame: {e}")

def get_mv_quote(symbol: str):
    """
    Get a detailed quote for the specified symbol.
    Includes all the quote attributes available in the VBA version.
    """
    con = connect_to_mv_com_server()
    if con is None:
        raise RuntimeError("Failed to connect to MV COM server.")
    
    try:
        quote = con.GetQuote(symbol)
        
        # Create a dictionary containing all quote properties
        quote_data = {
            "Last": getattr(quote, "Last", None),
            "NetChange": getattr(quote, "NetChange", None),
            "PercentChange": getattr(quote, "PercentChange", None),
            "High": getattr(quote, "High", None),
            "Low": getattr(quote, "Low", None),
            "Open": getattr(quote, "Open", None),
            "Close": getattr(quote, "Close", None),
            "Settle": getattr(quote, "Settle", None),
            "Bid": getattr(quote, "Bid", None),
            "Ask": getattr(quote, "Ask", None),
            "TradeSize": getattr(quote, "TradeSize", None),
            "OpenInterest": getattr(quote, "OpenInterest", None),
            "TradeDateTimeUtc": getattr(quote, "TradeDateTimeUtc", None),
            "Volume": getattr(quote, "Volume", None),
            "PrevPrice": getattr(quote, "PrevPrice", None),
            "TickCount": getattr(quote, "TickCount", None),
            "ContractDate": getattr(quote, "ContractDate", None),
            "ExpirationDate": getattr(quote, "ExpirationDate", None),
            "MidPoint": getattr(quote, "MidPoint", None),
            "CloseDate": getattr(quote, "CloseDate", None),
            "Currency": getattr(quote, "Currency", None),
            "LotUnit": getattr(quote, "LotUnit", None),
            "PutCall": getattr(quote, "PutCall", None),
            "Strike": getattr(quote, "Strike", None),
            "SettleDate": getattr(quote, "SettleDate", None),
            "Underlier": getattr(quote, "Underlier", None),
            "BidDateTimeUtc": getattr(quote, "BidDateTimeUtc", None),
            "BidSize": getattr(quote, "BidSize", None),
            "AskDateTimeUtc": getattr(quote, "AskDateTimeUtc", None),
            "AskSize": getattr(quote, "AskSize", None),
            "PrevHigh": getattr(quote, "PrevHigh", None),
            "PrevLow": getattr(quote, "PrevLow", None),
            "PrevOpen": getattr(quote, "PrevOpen", None),
            "PrevClose": getattr(quote, "PrevClose", None),
            "PrevVol": getattr(quote, "PrevVol", None),
            "MostRecentValue": getattr(quote, "MostRecentValue", None),
            "MostRecentValueDate": getattr(quote, "MostRecentValueDate", None),
            "Description": getattr(quote, "Description", None),
        }
        
        return quote_data
    except Exception as e:
        raise RuntimeError(f"Failed to get quote data for {symbol}: {e}")

def fetch_user_defined_formulas():
    """
    Fetch user defined formulas as shown in the VBA List_Click() function.
    Returns a DataFrame with the formulas.
    """
    con = connect_to_mv_com_server()
    if con is None:
        raise RuntimeError("Failed to connect to MV COM server.")
    
    try:
        formulas_raw = con.GetUserDefinedFormulas()
        formulas_data = []
        
        for formula in formulas_raw:
            formula_info = {
                "Folder": getattr(formula, "Folder", None),
                "Symbol": getattr(formula, "Symbol", None),
                "Description": getattr(formula, "Description", None),
                "Definition": getattr(formula, "Definition", None),
            }
            formulas_data.append(formula_info)
            
        return pd.DataFrame(formulas_data)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch user defined formulas: {e}")

def save_user_defined_formula(symbol: str, description: str, folder: str, definition: str):
    """
    Save a user defined formula as shown in the VBA Save_Click() function.
    """
    con = connect_to_mv_com_server()
    if con is None:
        raise RuntimeError("Failed to connect to MV COM server.")
    
    try:
        result = con.SaveUserDefinedFormula(Symbol=symbol, Description=description, 
                                           Folder=folder, Definition=definition)
        return result
    except Exception as e:
        raise RuntimeError(f"Failed to save user defined formula: {e}")

def test_daily_data_pull():
    """Test pulling daily data for /GCL over a short time range."""
    symbol = r"/GCL"  # raw string to handle backslash
    end_date = datetime.now()
    start_date = end_date.replace(day=max(1, end_date.day - 7))  # Just go 7 days back

    print("\n--- Testing Daily Data Pull ---")
    try:
        df = get_mv_data(symbol=symbol, data_type='daily', start_date=start_date, end_date=end_date, inspect_first=True)
        print("Daily Data pulled successfully:")
        print(df.head())
        return "successful_pull"
    except Exception as e:
        print(f"Test daily data pull failed: {e}")
        return "unsuccessful_pull"

def test_options_data_pull():
    """Test pulling options chain data with both /GCLN25 and /NG for comparison with VBA."""
    symbols = [r"/GCLN25", r"/NG"] 
    strike_count = 5  # Number of strikes to retrieve

    print("\n--- Testing Options Data Pull ---")
    try:
        # Test multiple symbols to ensure compatibility with VBA implementation
        for symbol in symbols:
            print(f"\nTesting option chain for {symbol}")
            df = get_mv_data(symbol=symbol, data_type='option_chain', strike_num=strike_count, inspect_first=True)
            print(f"Options Data for {symbol} pulled successfully:")
            print(df.head())
        return "successful_pull"
    except Exception as e:
        print(f"Test options data pull failed: {e}")
        return "unsuccessful_pull"

def test_quote_data_pull():
    """Test pulling quote data for /GCL."""
    symbol = r"/GCL"  # raw string to handle backslash

    print("\n--- Testing Quote Data Pull ---")
    try:
        quote_data = get_mv_quote(symbol)
        print("Quote Data pulled successfully:")
        for key, value in quote_data.items():
            print(f"{key}: {value}")
        return "successful_pull"
    except Exception as e:
        print(f"Test quote data pull failed: {e}")
        return "unsuccessful_pull"

if __name__ == "__main__":
    # Run tests for various data types
    daily_status = test_daily_data_pull()
    options_status = test_options_data_pull()
    quote_status = test_quote_data_pull()

    if daily_status == "successful_pull" and options_status == "successful_pull" and quote_status == "successful_pull":
        print("\nAll tests completed successfully!")
    else:
        print("\nSome tests failed. Check the error messages above.")