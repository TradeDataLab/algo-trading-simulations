import pandas as pd
import ta

def calculate_indicators(df, windows):
    window_sr, window_t = windows
    ta_stochrsi = ta.momentum.StochRSIIndicator(close=df.close, window=window_sr, smooth1=3, smooth2=3)
    df['stoch_rsi'] = ta_stochrsi.stochrsi()
    ta_trix= ta.trend.TRIXIndicator(close=df.close, window=window_t)
    df['trix'] = ta_trix.trix()
    df.dropna(inplace=True)
    
    return df

def buy_sell_signals(df):
    buy_condition = (df['stoch_rsi'] >= 0.8) & (df['stoch_rsi'].shift(1) < 0.8) & (df['trix'] >= 0.0) & (df['trix'].shift(1) < 0.0)   
    sell_condition = (df['stoch_rsi'] < 0.2) & (df['stoch_rsi'].shift(1) >= 0.2) & (df['trix'] < 0.0) & (df['trix'].shift(1) >= 0.0)  
    # Add columns
    df['signals_buy'] = df.loc[buy_condition, 'close']
    df['signals_sell'] = df.loc[sell_condition, 'close']
    
    return df

def evaluate_strategy(df):
    
    buys_list = df['signals_buy'].dropna().to_list()
    sells_list = df['signals_sell'].dropna().to_list()
    
    if len(buys_list) == 0 or len(sells_list) == 0:
       average_difference = None 
    else:
        average_difference =  (sum(sells_list) / len(sells_list)) - (sum(buys_list) / len(buys_list))
    
    return average_difference

df_raw = pd.read_pickle("./BTC_USDT.pkl")

def simmulate_combinations(windows):
    eval = evaluate_strategy(buy_sell_signals(calculate_indicators(df_raw, windows)))
    window_sr, window_t = windows
     
    return {'average_difference' : eval,'window_stoch_rsi' : window_sr,'window_trix' : window_t}