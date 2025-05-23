from seasonalFunctions import *
from itertools import product
import pandas as pd 
from datetime import datetime, timedelta
import ast
from sqlalchemy import create_engine, text
from urllib import parse
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv("credential.env")

schemaName = 'Reference'
table_Name = 'FuturesExpire'

# Get connection parameters from environment variables
connection_params = {
    "server": os.getenv("DB_SERVER"),
    "database": os.getenv("DB_NAME"),
    "username": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
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


#%% Expire Schedule 
futuresContractDict= {'F':{'abr':'Jan','num':1},'G':{'abr':'Feb','num':2},'H':{'abr':'Mar','num':3},'J':{'abr':'Apr','num':4},
                      'K':{'abr':'May','num':5},'M':{'abr':'Jun','num':6},'N':{'abr':'Jul','num':7},'Q':{'abr':'Aug','num':8},
                      'U':{'abr':'Sep','num':9},'V':{'abr':'Oct','num':10},'X':{'abr':'Nov','num':11},'Z':{'abr':'Dec','num':12}}

query = "SELECT * FROM [Reference].[FuturesExpire]" 

expire = pd.read_sql(query,con=engine)

#%%

df_out = pd.DataFrame({})

# Load the CSV using the first row as column headers
curvesIn = pd.read_csv("PriceAnalyzerIn.csv", header=0)

# Loop through each row in the DataFrame
for index, row in curvesIn.iterrows():
    
    print(row)
    
    variables = {}
    for name in curvesIn.columns:
        value = row[name]
        try:
            # Try to convert string representations of lists/dicts into Python objects
            parsed_value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            parsed_value = value
        variables[name] = parsed_value



    yearList = generateYearList(variables['contractMonthsList'], variables['yearOffsetList'])
    
    pricesDict,expireList = generate_contract_data(variables['tickerList'], variables['contractMonthsList'], yearList, variables['weightsList'], variables['convList'], variables['yearsBack'], conn)
    
    
    # # Example usage
    validate_contract_data(pricesDict)
    
    
    
    # Construct combined list to filter valid contracts
    combined_list = [variables['rollFlag'] + exp for exp in expireList]
    
    # Construct full ticker-month-year strings
    expire['TickerMonthYear'] = expire['Ticker'] + expire['MonthCode'] + expire['LastTrade'].str.slice(-2)
    
    # Filter expire matrix to only contracts in our target list
    expireMatrix = expire[expire['TickerMonthYear'].isin(combined_list)]
    
    # Construct front ticker label
    expireMatrix['frontTicker'] = variables['tickerList'][0] + expire['MonthCode'] + expire['LastTrade'].str.slice(-2)
    
    # Initialize a dictionary to store spreads keyed by year
    spread_dict = {}
    
    # Assume all product lists are same length
    num_contracts = len(next(iter(pricesDict.values()))["ContractList"])
    
    # Iterate over each contract index
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
    
        # Drop rows with missing values across the instruments
        combined_df.dropna(inplace=True)
        combined_df["spread"] = combined_df.sum(axis=1, skipna=True)
    
        # Extract year from contract suffix (e.g., Z25 → 2025)
        year_suffix = first_contracts[0][-2:]  # Last two characters
        spread_year = 2000 + int(year_suffix) if int(year_suffix) < 50 else 1900 + int(year_suffix)
    
        spread_dict[spread_year] = combined_df
        
        
    expireMatrix["LastTrade"] = pd.to_datetime(expireMatrix["LastTrade"])
    rows_to_drop = 5
    today = pd.Timestamp.today()
    
    # Create a mapping from year to last trade date (e.g., 2025 → Timestamp)
    expireMatrix["Year"] = expireMatrix["LastTrade"].dt.year
    year_to_last_trade = expireMatrix.set_index("Year")["LastTrade"].to_dict()
    
    # New dictionary to hold filtered spread data
    filtered_spread_dict = {}
    
    for year_key, df in spread_dict.items():
        if year_key in year_to_last_trade:
            last_trade_date = year_to_last_trade[year_key]
    
            # Keep rows up to and including LastTrade
            df = df[df.index <= last_trade_date]
    
            # Only drop last N rows if LastTrade is in the past
            if last_trade_date < today and len(df) > rows_to_drop:
                df = df.iloc[:-rows_to_drop]
    
            df['LastTrade'] = last_trade_date
            filtered_spread_dict[year_key] = df
    
    
    
    # Prepare spread-only DataFrame for database export
    combined_spread_list = []
    
    for year, df in filtered_spread_dict.items():
        if not df.empty and 'spread' in df.columns:
            df_copy = df[['spread', 'LastTrade']].copy()
            df_copy["Year"] = str(year)  # Use year as RollTicker
            df_copy["Date"] = df_copy.index
            combined_spread_list.append(df_copy.reset_index(drop=True))
    
    # Concatenate all into one DataFrame
    final_spread_df = pd.concat(combined_spread_list, ignore_index=True)
    
    # Reorder columns
    final_spread_df = final_spread_df[['Date', 'Year', 'spread', 'LastTrade']]
    
    # Add metadata fields
    final_spread_df['InstrumentName'] = variables['Name']
    final_spread_df['Group'] = variables['group']
    final_spread_df['Region'] = variables['region']
    final_spread_df['Month'] = variables['months']
    final_spread_df['RollFlag'] = variables['rollFlag']
    final_spread_df['Desc'] = variables['desc']

    df_out = pd.concat([df_out,final_spread_df],axis = 0)

with engine.begin() as connection:
    
   df_out.to_sql(name='contractMargins', schema='TradePriceAnalyzer', con=connection, if_exists='replace', index=False, chunksize=10000)

# # # with engine.begin() as connection:
    
# # #     final_spread_df.to_sql(name='contractMargins', schema='TradePriceAnalyzer', con=connection, if_exists='append', index=False, chunksize=10000)