import time
import multiprocessing
import os
from functools import partial

from tqdm import tqdm
import pandas as pd
import ccxt


download_folder = "data_raw"
data_folder = "data"

default_timeframe = "1m"

class OHLCV:
    def __init__(self, exchange: ccxt.Exchange):
        self.exchange = exchange
        self.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        
        os.makedirs(download_folder, exist_ok=True)
        os.makedirs(data_folder, exist_ok=True)
    
    def get_filename(self, symbol: str, timeframe: str) -> str:
        return f"ohlcv_{symbol.replace('/', '_')}_{timeframe}.pkl"
    
    def fetch(self, symbol: str, timeframe: str, since = 0) -> pd.DataFrame:
        pbar = tqdm(desc = f"Downloading {symbol:>8}")
    
        candles = []
        
        sleep_timer = 10

        while True:

            try:
                candles_new = self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since, limit=1000)
            except ccxt.errors.DDoSProtection:
                print(f"DDoSProtection protection for {symbol:>8} - sleeping for {sleep_timer} seconds...")
                time.sleep(sleep_timer)
                sleep_timer += 10
                continue
            
            if len(candles_new) == 0:
                break
            
            candles += candles_new

            since = candles[-1][0] + 1
            
            pbar.update(1)
        
        pbar.close()
            
        return pd.DataFrame(candles, columns=self.columns)

    def download_or_update_symbol_data(self, symbol: str, timeframe: str) -> None:
        
        filename = self.get_filename(symbol, timeframe)
        filepath = os.path.join(download_folder, filename)
                
        try:
            
            df = pd.read_pickle(filepath)
            
            # print(f"File {filepath} found. Updating...")
            since = df["timestamp"].iloc[-1] + 1
            
            df_new = self.fetch(symbol, timeframe, since)
            if df_new.shape[0] > 0:
                df = pd.concat([df, df_new], ignore_index = True)
                df.to_pickle(filepath)
            
        except FileNotFoundError:
            
            # print(f"File {filepath} not found. Downloading...")
            df = self.fetch(symbol, timeframe)
            df.to_pickle(filepath)
    
    def clean(self, symbol: str, timeframe: str) -> None:
        
        infile = os.path.join(download_folder, self.get_filename(symbol, timeframe))
        outfile = os.path.join(data_folder, self.get_filename(symbol, timeframe))
        
        # assert file exists
        assert os.path.isfile(infile), f"File {infile} does not exist."        
        
        df = pd.read_pickle(infile)

        # Create a boolean mask to identify neighboring duplicate rows.
        # This filters out periods od time where no trades were made, due to exchange being down.
        # Can't use `drop_duplicates` as it would also detect duplicates across different time periods.
        not_time_columns = ['open', 'high', 'low', 'close', 'volume']
        mask = (df[not_time_columns] == df[not_time_columns].shift()).all(axis=1)

        # apply the mask and reset index
        df = df[(~mask)].reset_index(drop=True)
        
        # TODO convert timestamp to datetime and set as index depending on requirements of used backtesting library
        # df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
        
        df.to_pickle(outfile)
        
    def process_symbol(self, symbol: str, timeframe: str) -> None:
        self.download_or_update_symbol_data(symbol, timeframe)
        self.clean(symbol, timeframe)
        
    def process_symbols(self, symbols: str, timeframe: str = default_timeframe) -> None:
        # partial because multiprocessing.Pool.map only accepts functions with one argument
        with multiprocessing.Pool(processes=len(symbols)) as pool:
            func = partial(self.process_symbol, timeframe=timeframe)
            pool.map(func, symbols)
        
    # fetches data from file if it exists, otherwise downloads it
    def get_df(self, symbol: str, timeframe: str = default_timeframe, update: bool = False) -> pd.DataFrame:
        
        filename = self.get_filename(symbol, timeframe)
        filepath = os.path.join(data_folder, filename)
        
        # update if file does not exist
        if update or not os.path.isfile(filepath):
            self.download_or_update_symbol_data(symbol, timeframe)
            self.clean(symbol, timeframe)
        
        return pd.read_pickle(filepath)
