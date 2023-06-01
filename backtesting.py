import ta
import pandas as pd
import numpy as np
import itertools
from multiprocessing import Pool, cpu_count
import tqdm

import warnings
warnings.filterwarnings("ignore")

# Download data
def get_data():
    # Read pickle file
    df = pd.read_pickle("./data/ohlcv_BTC_USDT_1m.pkl")
    # Sort index
    df = df.sort_index(ascending=True)
    
    return df

# Backtesting class
class Backtester:
    def __init__(self, strategy, data, parameters, initial_cash, commission):
        self.strategy = strategy
        self.data = data
        self.parameters = parameters
        self.initial_cash = initial_cash
        self.commission = commission
        self.positions = None
        self.benchmark = None

    def get_positions(self):
        # Generate trading signals
        signals = self.strategy.generate_signals(self.data)  
        # Filter only timestamps with generated signals
        signals = signals[(signals['signals'] == -1) | (signals['signals'] == 1)]
        self.positions = signals

        return self.positions 

    def strategy_results(self):
        # Generate positions
        self.get_positions()
        # Calculate returns, add commission to every signal, if NaN fill with 0.0
        self.positions['returns'] = np.zeros
        self.positions['returns'] = (self.positions['close'] / self.positions['close'].shift()) - self.commission - 1
        self.positions.loc[(self.positions['returns'].isna(), 'returns')] = 0.0 - self.commission
        # Calculate log returns
        self.positions['log_returns'] = np.log(1 + self.positions['returns'])
        # Calculate balance
        self.positions.loc[self.positions.index[0], 'balance'] = self.initial_cash 
        self.positions.loc[self.positions.index[1]:, 'balance'] = self.initial_cash * np.exp(self.positions['returns'].cumsum()) 
        
        return self.positions 
    
    def buy_hold_results(self):
        # Get strategy returns
        self.strategy_results()
        # Generate data for benchmarking
        self.benchmark = self.data[['close']]
        # Create benchmark for buy and hold strategy and fill NaN with 0.0
        self.benchmark['returns_bh'] = (self.benchmark['close'] / self.benchmark['close'].shift()) -  1 
        self.benchmark.loc[(self.benchmark['returns_bh'].isna(), 'returns_bh')] = 0.0 
        # Introduce commission to first and last timestamp
        self.benchmark.loc[self.benchmark.index[[0, -1]], 'returns_bh'] = self.benchmark.loc[self.benchmark.index[[0, -1]], 'returns_bh'] - self.commission
        # Calculate log returns for buy and hold strategy
        self.benchmark['log_ret_bh'] = np.log(1 + self.benchmark['returns_bh'])
        
        return self.benchmark  
    
    def evaluation_function(self):
        # Get strategy and benchmark returns
        self.buy_hold_results()
        # Strategy evaluation metrics
        return_multiplier_strategy = np.exp(self.positions['log_returns'].sum()) - 1
        # Benchmark evaluation metrics
        return_multiplier_bh = np.exp(self.benchmark['log_ret_bh'].sum()) - 1
        
        return return_multiplier_strategy, return_multiplier_bh

    def evaluate_strategy(self, params):
        # Get parameters for strategy class
        self.strategy.set_parameters(**dict(zip(self.parameters.keys(), params)))
        # Get strategy and benchmark returns
        return_multiplier, return_multiplier_bh = self.evaluation_function()
        # Create a dictionary with combinations and return multipliers
        eval_dict = dict(zip(self.parameters.keys(), params))
        eval_dict['return_multiplier_strategy'] = return_multiplier
        eval_dict['return_multiplier_bh'] = return_multiplier_bh
        
        self.reset_backtest()
        
        return eval_dict
    
    
    def run_simulation(self):
        # Get combinations of parameters
        param_combinations = list(itertools.product(*self.parameters.values()))
        param_combinations = [combination for combination in param_combinations if combination[0] != combination[1]]
        # Conduxt backtesting with multiprocessing
        with Pool(cpu_count()) as pool:
            results_list = list(tqdm.tqdm(pool.imap(self.evaluate_strategy, param_combinations), total=len(param_combinations)))
            results_df = pd.DataFrame(results_list)
            
        return results_df
    
    def reset_backtest(self):
        self.positions = None

#parameter_ranges = {"window_1": range(2, 11), "window_2": range(2, 11)}


# SMA strategy class
class SMAstrategy:
    def __init__(self, window_1, window_2):
        self.window_1 = window_1
        self.window_2 = window_2
        
    def set_parameters(self, window_1, window_2):
        self.window_1 = window_1
        self.window_2 = window_2

    def generate_signals(self, data):
        self.data = data
        # Genearete 2 SMAs and drop NaN
        self.data['sma_1'] = ta.trend.sma_indicator(self.data['close'], window=self.window_1)
        self.data['sma_2'] = ta.trend.sma_indicator(self.data['close'], window=self.window_2)
        self.data = self.data.dropna()
        # BUY signals
        self.data.loc[(self.data['sma_1'] <= self.data['sma_2']) & (self.data['sma_1'].shift(1) > self.data['sma_2'].shift(1)), 'signals'] = 1
        # SELL signals
        self.data.loc[(self.data['sma_1'] >= self.data['sma_2']) & (self.data['sma_1'].shift(1) < self.data['sma_2'].shift(1)), 'signals'] = -1 
        # Limit the data so that it starts with the BUY signal and ends with the SELL signal
        first_buy = self.data[self.data['signals'] == 1].index[0]
        last_sell = self.data[self.data['signals'] == -1].index[-1]
        self.data = self.data.loc[first_buy:last_sell,]
        # Fill non signal timestamps with 0
        self.data['signals'] = self.data['signals'].fillna(0)
        
        return self.data[['close', 'signals']]