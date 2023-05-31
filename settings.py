import os

DOWNLOAD_FOLDER = "data_raw"
DATA_FOLDER = "data"
        
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
# os.makedirs(DATA_FOLDER, exist_ok=True)

# DEFAULT_TIMEFRAME = "1m"

# evaluations_filename = "evaluations.csv"

symbols = [
    "BTC/USDT",
    "ETH/USDT",
    "BNB/USDT",
    "ADA/USDT",
    "DOGE/USDT",
    "XRP/USDT",

    "ETH/BTC",
    "BNB/BTC",
    "ADA/BTC",
    "DOGE/BTC",
    "XRP/BTC",
]

timeframes = [
    "1m",
    "5m",
    "15m",
    # "30m",
    "1h",
    # "4h",
    "1d",
]