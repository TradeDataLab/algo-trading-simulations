import pandas as pd
import numpy as np
import ta

# Download data
def get_data():
    # Read pickle file
    df = pd.read_pickle("./data/ohlcv_BTC_USDT_1m.pkl")
    # Choose columns
    cols = ['time', 'close']
    df = df[cols]
    # Set index to time
    df.set_index('time', inplace=True)
    # Sort index
    df = df.sort_index(ascending=True)
    
    return df

########## SMA Simmulation ##########

# Calculate SMAs of desired lengths
def calculate_sma(df, windows):
    # SMAs instances
    ta_sma_short = ta.trend.SMAIndicator(close=df['close'], window=windows[0])
    df['sma_short'] = ta_sma_short.sma_indicator() 
    ta_sma_long = ta.trend.SMAIndicator(close=df['close'], window=windows[1])
    df['sma_long'] = ta_sma_long.sma_indicator() 
    # Drop NaN values
    df = df.dropna().reset_index(drop=True)
    
    return df

# Determine BUY/SELL signals
def buy_sell_signals_sma(df):
    # Set conditions for BUY/SELL signals (for SMAs)
    buy_condition = (df['sma_long'] >= df['sma_short']) & (df['sma_long'].shift(1) < df['sma_short'].shift(1))  
    sell_condition = (df['sma_long'] < df['sma_short']) & (df['sma_long'].shift(1) >= df['sma_short'].shift(1))
    # Add columns with signals BUY/SELL signals
    df['signals_buy'] = df.loc[buy_condition, 'close']
    df['signals_sell'] = df.loc[sell_condition, 'close']
    
    return df

# Evaluate generated BUY/SELL signals
def evaluate_strategy_sma(df):
    # Drop null values from 
    signals_buy = df['signals_buy'].dropna()
    signals_sell = df['signals_sell'].dropna()
    # Create profit multipliers list
    multipliers_list = []
    # Append to multiplier list in chronogical manner
    for s, b in zip(signals_sell, signals_buy):
        multiplier = s / b
        multipliers_list.append(multiplier)
    # Convert to pd.Series
    multipliers = pd.Series(multipliers_list, index = range(len(multipliers_list)))
    # Take last observation as overall profit multiplier
    overall_profit_multiplier = multipliers.cumprod().iloc[-1]
    # Calculate average profit multiplier for each pair of BUY/SELL signals
    average_profit_multiplier = signals_sell.mean() / signals_buy.mean()
    # Count trade pairs
    buy_sell_trade_pair_count = (signals_buy.count() + signals_sell.count()) / 2
            
    return overall_profit_multiplier, average_profit_multiplier, buy_sell_trade_pair_count
    

df_raw = get_data() # instatntiate raw data for combinations simmulation

# Simmulate combinations 
def simmulate_combinations_sma(windows):
    # Calculate evaluation metrics for different combinations
    eval1, eval2, eval3 = evaluate_strategy_sma(buy_sell_signals_sma(calculate_sma(df_raw, windows)))
     
    return {'window_sma_short': windows[0], 'window_sma_long': windows[1], 'overall_profit_multiplier': eval1, 'average_profit_multiplier': eval2, 'buy_sell_trade_pair_count': eval3}

########## SMA / Stoch RSI / TRIX Simmulation ##########
    
# Calculate indicators for SMAs/TRIX strategy
def calculate_trix(df, sma, trix):
    # SMAs instances
    ta_sma_short = ta.trend.SMAIndicator(close=df['close'], window=sma[0])
    df['sma_short'] = ta_sma_short.sma_indicator() 
    ta_sma_long = ta.trend.SMAIndicator(close=df['close'], window=sma[1])
    df['sma_long'] = ta_sma_long.sma_indicator() 
    # TRIX instance
    ta_trix= ta.trend.TRIXIndicator(close=df['close'], window=trix)
    df['trix'] = ta_trix.trix()
    # Drop NaN values
    df = df.dropna().reset_index(drop=True)
    
    return df

# Determine BUY/SELL signals for SMAs/TRIX strategy
def buy_sell_signals_trix(df):
    # Set conditions for BUY/SELL signals
    buy_condition = (df['sma_long'] >= df['sma_short']) & (df['sma_long'].shift(1) < df['sma_short'].shift(1)) & (df['trix'] >= 0.0)  
    sell_condition = (df['sma_long'] < df['sma_short']) & (df['sma_long'].shift(1) >= df['sma_short'].shift(1)) & (df['trix'] >= 0.0) 
    # Add columns with signals BUY/SELL signals
    df['signals_buy'] = df.loc[buy_condition, 'close']
    df['signals_sell'] = df.loc[sell_condition, 'close']
    
    return df

# filter generated signals to keep only first in a row
def filter_signals(df):
    # Filter for cell where signals have been generated
    df_filter = df[(df['signals_buy'].notnull()) | (df['signals_sell'].notnull())]
    # Choose columns for signals filtering
    cols_list = ['signals_buy', 'signals_sell']
    # Loop through
    for col in cols_list:
        # Create mask of non-null cells
        not_null_mask = df_filter[col].notnull()
        # Create mask where each cell is True if the next cell is not null
        next_not_null_mask = not_null_mask.shift(-1, fill_value=False)
        # Set cells to null where the next cell is not null
        df_filter.loc[next_not_null_mask, col] = np.nan
        # Assign new values to main dataframe
        df['signals_buy'] = df_filter['signals_buy']
        df['signals_sell'] = df_filter['signals_sell']
        
    return df

# Evaluate generated BUY/SELL signals
def evaluate_strategy_trix(df):
    # Filter signals to keep only first signal in a row
    df = filter_signals(df)
    # Unpack evaluation metrics
    overall_profit_multiplier, average_profit_multiplier, buy_sell_trade_pair_count = evaluate_strategy_sma(df)
     
    return overall_profit_multiplier, average_profit_multiplier, buy_sell_trade_pair_count

sma = (2, 11) # Best combination of SMAs generated

# Simmulate combinations for SMAs/TRIX strategy
def simmulate_combinations_trix(trix):
    # Calculate evaluation for different combinations
    eval1, eval2, eval3 = evaluate_strategy_trix(buy_sell_signals_trix(calculate_trix(df_raw, sma, trix)))
     
    return {'window_sma_short': sma[0], 'window_sma_long': sma[1], 'window_trix': trix,'overall_profit_multiplier': eval1, 'average_profit_multiplier': eval2, 'buy_sell_trade_pair_count': eval3}