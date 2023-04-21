
import os
import multiprocessing

from tqdm import tqdm
import pandas as pd

from download import download_folder, symbols, get_ohlcv_filename

# TODO move this to config
data_folder = "data"
os.makedirs(data_folder, exist_ok=True)

def remove_duplicate_rows(df):

    # Create a boolean mask to identify neighboring duplicate rows with volume greater than 0.
    # This filters out periods od time where no trades were made, due to exchange being down.
    # Can't use `drop_duplicates` as it would also detect duplicates across different time periods.
    not_time_columns = ['open', 'high', 'low', 'close', 'volume']
    mask = (df[not_time_columns] == df[not_time_columns].shift()).all(axis=1)

    # apply the mask and reset index
    df =  df[(~mask)].reset_index(drop=True)
    return df


def clean_symbol(symbol):
    
    filename = get_ohlcv_filename(symbol, "1m")
    infile = os.path.join(download_folder, filename)
    outfile = os.path.join(data_folder, filename)
    
    df_raw = pd.read_pickle(infile)
    
    df = remove_duplicate_rows(df_raw)
    
    df["time"] = pd.to_datetime(df["timestamp"], unit="ms")
    
    df.to_pickle(outfile)

def clean_symbols():    
    with multiprocessing.Pool(processes=len(symbols)) as pool:
        for _ in tqdm(pool.imap_unordered(clean_symbol, symbols)):
            pass
    
if __name__ == "__main__":
    clean_symbols()
