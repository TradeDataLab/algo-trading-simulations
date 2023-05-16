import os
import time

from tqdm import tqdm
import ccxt
import pandas as pd

from settings import DOWNLOAD_FOLDER, DATA_FOLDER

class OHLCV:
    def __init__(self, exchange: ccxt.Exchange, symbol: str, timeframe: str):
        """OHLCV data handler.
        
        Args:
            exchange (ccxt.Exchange): ccxt exchange object.
            symbol (str): symbol to fetch.
            timeframe (str): timeframe to fetch. 
        """
        
        self.exchange = exchange
        self.symbol = symbol
        self.timeframe = timeframe
        
        self.filename = f"ohlcv_{self.symbol.replace('/', '_')}_{self.timeframe}.pkl"
        self.filepath_raw = os.path.join(DOWNLOAD_FOLDER, self.filename)
        self.filepath_clean = os.path.join(DATA_FOLDER, self.filename)
        
        if not os.path.isfile(self.filepath_clean):
            print(f"File {self.filepath_clean} not found. Downloading...")
            self.__update()
        
        self.data = self.get_data(update=False)
        
    def __fetch_candles(self, since: int = 0) -> list:
        """Fetches candles from exchange.
        
        Args:
            since (int, optional): timestamp to start fetching from. Defaults to 0.
        """
        
        pbar = tqdm(desc = f"Fetching {self.symbol:>8} candles")
    
        candles = []
        
        sleep_timer = 10

        while True:

            try:
                candles_new = self.exchange.fetch_ohlcv(symbol=self.symbol, timeframe=self.timeframe, since=since, limit=1000)
            except ccxt.errors.DDoSProtection:
                print(f"DDoSProtection protection for {self.symbol:>8} - sleeping for {sleep_timer} seconds...")
                time.sleep(sleep_timer)
                sleep_timer += 10
                continue
            
            if len(candles_new) == 0:
                pbar.close()
                return candles
            
            candles += candles_new

            since = candles[-1][0] + 1
            
            pbar.update(1)
            
    def __parse_candles(self, candles: list) -> pd.DataFrame:
        """Parses candles into a pandas DataFrame.
        
        Args:
            candles (list): list of candles.
        """
        
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(candles, columns = columns)
        
        return df
        
    def __update(self):
        """Updates the raw data file."""
        
        try:
            
            df = pd.read_pickle(self.filepath_raw)
            
            # print(f"File {filepath} found. Updating...")
            since = df["timestamp"].iloc[-1] + 1
            
            candles = self.__fetch_candles(since)
            df_new = self.__parse_candles(candles)
            if df_new.shape[0] > 0:
                df = pd.concat([df, df_new], ignore_index = True)
                df.to_pickle(self.filepath_raw)
            
        except FileNotFoundError:
            
            # print(f"File {filepath} not found. Downloading...")
            candles = self.__fetch_candles()
            df = self.__parse_candles(candles)
            df.to_pickle(self.filepath_raw)
            
        self.__clean()
    
    def __clean(self) -> None:
        """Reads raw data, removes duplicate rows and saves the cleaned data."""
        
        df = pd.read_pickle(self.filepath_raw)

        # Create a boolean mask to identify neighboring duplicate rows.
        # This filters out periods od time where no trades were made, due to exchange being down.
        # Can't use `drop_duplicates` as it would also detect duplicates across different time periods.
        not_time_columns = ['open', 'high', 'low', 'close', 'volume']
        mask = (df[not_time_columns] == df[not_time_columns].shift()).all(axis=1)

        # apply the mask and reset index
        df = df[(~mask)].reset_index(drop=True)
        
        # TODO convert timestamp to datetime and set as index depending on requirements of used backtesting library
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index('timestamp', inplace=True)
        
        df.to_pickle(self.filepath_clean)
    
    def get_data(self, update: bool = False) -> pd.DataFrame:
        """Returns the cleaned data.
        
        Args:
            update (bool, optional): whether to update the data. Defaults to False.
            
        Returns:
            pd.DataFrame: cleaned data.
        """
        
        if update:
            self.__update()
        
        df = pd.read_pickle(self.filepath_clean)
        
        return df
