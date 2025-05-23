from GvWSConnection import *
from datetime import datetime as dt
import pandas as pd 
from datetime import timedelta, datetime as dt
import sys
from gcc_sparta_library import get_mv_data

USERNAME = 'GCC018'
PASSWORD = 'password'

conn = GvWSConnection(USERNAME, PASSWORD)


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