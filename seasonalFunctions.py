from GvWSConnection import *
from datetime import datetime as dt
import pandas as pd
from datetime import timedelta, datetime as dt
import sys
from gcc_sparta_library import get_mv_data
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv("credential.env")

GvWSUSERNAME = os.getenv("GvWSUSERNAME")
GvWSPASSWORD = os.getenv("GvWSPASSWORD")

conn = GvWSConnection(GvWSUSERNAME, GvWSPASSWORD)

def generateYearList(contractMonthsList, yearOffsetList):
    if len(contractMonthsList) != len(yearOffsetList):
        raise ValueError("contractMonthsList and yearOffsetList must be the same length.")

    if any(offset < 0 for offset in yearOffsetList):
        raise ValueError("yearOffsetList cannot contain negative values.")

    current_year = dt.today().year
    current_month = dt.today().month

    year_list = []
    futuresContractDict= {'F':{'abr':'Jan','num':1},'G':{'abr':'Feb','num':2},'H':{'abr':'Mar','num':3},'J':{'abr':'Apr','num':4},
                      'K':{'abr':'May','num':5},'M':{'abr':'Jun','num':6},'N':{'abr':'Jul','num':7},'Q':{'abr':'Aug','num':8},
                      'U':{'abr':'Sep','num':9},'V':{'abr':'Oct','num':10},'X':{'abr':'Nov','num':11},'Z':{'abr':'Dec','num':12}}

    for i, contract_month_code in enumerate(contractMonthsList):
        offset = yearOffsetList[i]
        target_year = current_year + offset

        # Get the month number from the futuresContractDict
        contract_month_num = None
        for key, value in futuresContractDict.items():
            if key == contract_month_code:
                contract_month_num = value['num']
                break
        
        if contract_month_num is None:
            raise ValueError(f"Invalid contract month code: {contract_month_code}")

        # If the contract month has already passed in the current year, increment the target year
        if contract_month_num <= current_month:
            target_year += 1
        
        year_list.append(str(target_year)[-2:])

    return year_list


def contractMonths(expireIn,contractRollIn,ContractMonthIn):

    tempExpire = expireIn[expireIn['Ticker']==contractRollIn]

    tempExpire.set_index(pd.to_datetime(tempExpire['LastTrade'], format='%m/%d/%y'), inplace = True)
    filtered_contracts = tempExpire[tempExpire.index > dt.today()].copy()

    expireDate = filtered_contracts[filtered_contracts['MonthCode'] == ContractMonthIn]

    return expireDate.iloc[0, :]



# Modified generate_contract_data function
def generate_contract_data(tickerList, contractMonthsList, yearList, weightsList, convList, yearsBack, conn): # Renamed 'ticker' to 'tickerList' for clarity
    contract_data = {}

    # Calculate the past date (yearsBack + 2 years)
    past_date = dt.today().replace(year=dt.today().year - (yearsBack + 2))

    # Format it as 'MM/DD/YYYY'
    startDate = past_date.strftime('%m/%d/%Y')

    expireList = None  # Initialize expireList

    # Iterate through the lists based on index to ensure correct pairing
    for i in range(len(tickerList)): # Iterate using range(len(tickerList))
        t = tickerList[i] # Current ticker symbol
        contractMonth = contractMonthsList[i] # Current contract month code
        startYear = int(yearList[i]) # Current start year
        weight = weightsList[i] # Current weight
        conv = convList[i] # Current conversion factor

        # Create a unique key for each leg by combining ticker and contract month
        unique_key = f"{t}{contractMonth}"

        # Generate list of contracts going back 'yearsBack' years
        contractList = [f"{t}{contractMonth}{str(startYear - y).zfill(2)}" for y in range(yearsBack)]
        
        # Retrieve daily prices
        d1 = conn.get_daily(contractList, start_date=dt.strptime(startDate, '%m/%d/%Y'))
        
        # Convert to DataFrame
        df = pd.DataFrame(d1)
        
        # Rename columns properly
        df.rename(columns={'pricesymbol': 'symbol', 'tradedatetimeutc': 'Date'}, inplace=True)
        
        # Select only the required columns
        df = df.loc[:, ['symbol', 'Date', 'close']]
        
        # Compute weighted price
        df['WeightedPrice'] = df['close'] * conv * weight # Use individual conv and weight
        
        # Store data in dictionary using the unique key
        contract_data[unique_key] = { # Use unique_key here
            'Prices df': df,
            "ContractList": contractList,
            "Weights": weight, # Use individual weight
            "Conversion": conv # Use individual conv
        }

        # If it's the first leg (not just the first ticker), populate expireList
        # This part of expireList generation needs to be reconsidered if expireList is expected to be a comprehensive list of *all* contract expiry month/year suffixes,
        # otherwise, it will only contain suffixes from the first leg's contracts.
        if i == 0:
            expireList = [c[-3:] for c in contractList] # Example: ['F25', 'F24', ...]

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
        contractList = [f"{t}{contractMonth}{str(startYear - y).zfill(2)}" for y in range(yearsBack)] # Changed to zfill(2) to ensure two digits

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
                import time # Import time module
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