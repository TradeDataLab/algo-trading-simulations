import random
import time
import multiprocessing
import os

from tqdm import tqdm
import pandas as pd
import ccxt

download_folder = "data_raw"
os.makedirs(download_folder, exist_ok=True)

symbols = [
    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT',
    'ETH/BTC', 'BNB/BTC', 'XRP/BTC', 'ADA/BTC'
]

exchange = ccxt.binance()

def get_ohlcv_filename(symbol: str, timeframe: str):
    # TODO use path module
    return f"data_raw/ohlcv_{symbol.replace('/', '_')}_{timeframe}.pkl"

# lock for printing without text overlapping in a single line in multiprocessing
lock = multiprocessing.Lock()
def print_with_lock(text):
    with lock:
        print(text)


# TODO check if this works without this position variable
position = 0

def get_symbol_data(symbol, timeframe, since = 0):
    
    time.sleep(1)

    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    
    global position
    pbar = tqdm(desc = f"Downloading {symbol:>8}", position=position)
    position += 1
    
    candles = []

    while True:

        try:
            candles_new = exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=1000)
        except ccxt.errors.DDoSProtection:
            # wait random time. scaled by symbols
            sleep = random.randrange(20 * len(symbols))
            time.sleep(sleep)
            print_with_lock(f"DDoSProtection protection for {symbol:>8} - sleeping for {sleep} seconds...")
            continue
        
        if len(candles_new) == 0:
            break
        
        candles += candles_new

        since = candles[-1][0] + 1
        
        pbar.update(len(candles_new))
    
    pbar.close()
        
    return pd.DataFrame(candles, columns=columns)


def download_or_update_symbol_data(symbol, timeframe = "1m"):
    
    filename = get_ohlcv_filename(symbol, timeframe)
    
    try:
        
        df = pd.read_pickle(filename)
        
        print_with_lock(f"File {filename} found. Updating...")
        since = df["timestamp"].iloc[-1] + 1
        
        df_new = get_symbol_data(symbol, timeframe, since)
        if df_new.shape[0] > 0:
            df = pd.concat([df, df_new], ignore_index = True)
            df.to_pickle(filename)
        
    except FileNotFoundError:
        
        print_with_lock(f"File {filename} not found. Downloading...")
        df = get_symbol_data(symbol, timeframe)
        df.to_pickle(filename)
        
    return df


def main():
    # TODO should multiproccessing be used here? maybe threading since it's mostly IO bound
    with multiprocessing.Pool(processes=len(symbols)) as pool:
        for _ in pool.map(download_or_update_symbol_data, symbols):
            pass

if __name__ == "__main__":
    main()
