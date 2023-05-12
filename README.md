# algo-trading-simulations

Python scripts for downloading historic cryptocurrency data and running backtests on them.

## Init

Create and activate conda env
```bash
conda create --name ats python=3.11
conda activate ats
```

Install requirements
```bash
pip install -r requirements.txt
```

## Usage

```python
import ccxt

from OHLCV import OHLCV

exchange = ccxt.binance()
ohclv = OHLCV(exchange)

df = ohclv.get_df("BTC/USDT", "1m", update = True)
```

## Helpful snippets

```bash
pip install ipykernel tqdm pandas ccxt pandas_ta ta
pip install matplotlib plotly
pip list --format=freeze > requirements.txt
```
