# Import libraries
import vectorbt as vbt
import pandas as pd
import numpy as np
import ta
import itertools
import tqdm
from multiprocessing import Pool, cpu_count
import plotly.graph_objects as go

# Download data
def get_data() -> pd.DataFrame:
    # Read pickle file
    df = pd.read_pickle("./data/ohlcv_BTC_USDT_1m.pkl")
    # Sort index
    df = df.sort_index(ascending=True)
    
    return df

# Set class for trading strategy backtesting
class Backtesting:
    def __init__(self, strategy, close_price: pd.Series, parameters: dict, commission: float):
        """Backtesting class for trading strategy backtesting.
        
        Args:
            strategy (class): strategy class returning positions as np.array.
            close_price (pd.Series): symbol to fetch.
            parameters (str): parameters for strategy backtesting. 
            commission (float): percent of commission.
        """
        self.strategy = strategy
        self.close_price = close_price
        self.parameters = parameters
        #self.initial_cash = initial_cash
        self.commission = commission
        self.positions = None
        #self.benchmark = None
        
    def get_positions(self) -> np.array:
        """Generate positions for backtested strategy."""
        # Generate trading signals
        self.positions = self.strategy.generate_signals(self.close_price)  

        return self.positions 
    
    def evaluate_strategy(self, parameters: dict) -> dict:
        """Evaluate backtested strategy.
        
        Args:
            parameters (class): dictionary with combinations of parameters.
        """
        # Get parameters for strategy class
        self.strategy.set_parameters(**dict(zip(self.parameters.keys(), parameters)))
        # Generate positions
        self.get_positions()
        # Create sets of signals to enter and exit positions
        entries = self.positions == 1.0
        exits = self.positions == -1.0
        # Create Portfolio object for backtesting the strategy
        pf = vbt.Portfolio.from_signals(self.close_price, entries=entries, exits=exits, freq='m', fees=self.commission)
        # Create a dictionary with combinations and return multipliers
        eval_dict = dict(zip(self.parameters.keys(), parameters))
        eval_dict['return_multiplier_strategy'] = pf.total_return()
        #eval_dict['return_multiplier_bh'] = pf.total_benchmark_return()
    
        return eval_dict
        
    def run_simulation(self) -> pd.DataFrame:
        """Run backtesting."""
        # Get combinations of parameters
        param_combinations = list(itertools.product(*self.parameters.values()))
        param_combinations = [combination for combination in param_combinations if combination[0] != combination[1]]
        # Conduxt backtesting with multiprocessing
        with Pool(cpu_count() - 1) as pool:
            results_list = list(tqdm.tqdm(pool.imap(self.evaluate_strategy, param_combinations), total=len(param_combinations)))
            results_df = pd.DataFrame(results_list)
            
        return results_df
    
###### CUSTOM STRATEGY CLASSES ######    

# SMA crossover strategy class
class SMAstrategy:
    def __init__(self, window_sma_1: int, window_sma_2: int):
        """SMAstrategy class for generating SMAs crossover strategy positions.
        
        Args:
            window_sma_1 (int): length of 1st SMA window.
            window_sma_2 (int): length of 2nd SMA window.
        """
        self.window_sma_1 = window_sma_1
        self.window_sma_2 = window_sma_2
        
    def set_parameters(self, window_sma_1: int, window_sma_2: int):
        """Set parameters for currently tested combination.
        
        Args:
            window_sma_1 (int): length of 1st SMA window.
            window_sma_2 (int): length of 2nd SMA window.
        """
        self.window_sma_1 = window_sma_1
        self.window_sma_2 = window_sma_2

    def generate_signals(self, close_price: pd.Series) -> np.array:
        """Generate signals for BUY/SELL actions.
        
        Args:
            close_price (pd.Series): closing prices.
        """
        self.close_price = close_price
        # Calculate SMAs
        sma_1 = vbt.MA.run(close_price, window=self.window_sma_1)
        sma_2 = vbt.MA.run(close_price, window=self.window_sma_2)
        # Generate signals for SMAs crossovers
        entries_sma = sma_1.ma_crossed_above(sma_2).to_numpy()
        exits_sma = sma_1.ma_crossed_below(sma_2).to_numpy()
        # Generate positions
        positions = np.where((entries_sma == True), 1, 0) # BUY
        positions = np.where((exits_sma == True), -1, positions) # SELL
        
        return positions
    
# SMA/TRIX strategy class
class SMATRIXstrategy:
    def __init__(self, window_sma_1: int, window_sma_2: int, window_trix: int):
        """SMATRIXstrategy class for generating SMAs crossover 
        strategy positions, with TRIX indicator filter.
        
        Args:
            window_sma_1 (int): length of 1st SMA window.
            window_sma_2 (int): length of 2nd SMA window.
            window_trix (int): length of TRIX indicator window.
        """
        self.window_sma_1 = window_sma_1
        self.window_sma_2 = window_sma_2
        self.window_trix = window_trix 
        
    def set_parameters(self, window_sma_1: int, window_sma_2: int, window_trix: int):
        """Set parameters for currently tested combination.
        
        Args:
            window_sma_1 (int): length of 1st SMA window.
            window_sma_2 (int): length of 2nd SMA window.
            window_trix (int): length of TRIX indicator window.
        """
        self.window_sma_1 = window_sma_1
        self.window_sma_2 = window_sma_2
        self.window_trix = window_trix

    def generate_signals(self, close_price: pd.Series) -> np.array:
        """Generate signals for BUY/SELL actions.
        
        Args:
            close_price (pd.Series): closing prices.
        """
        self.close_price = close_price
        # Calculate SMAs
        sma_1 = vbt.MA.run(close_price, window=self.window_sma_1)
        sma_2 = vbt.MA.run(close_price, window=self.window_sma_2)
        # Generate signals for SMAs crossovers
        entries_sma = sma_1.ma_crossed_above(sma_2).to_numpy()
        exits_sma = sma_1.ma_crossed_below(sma_2).to_numpy()
        # Calculate TRIX indicator
        trix = ta.trend.TRIXIndicator(self.close_price, self.window_trix).trix().to_numpy()
        # Generate positions
        positions = np.where((entries_sma == True) & (trix > 0.0), 1, 0) # BUY
        positions = np.where((exits_sma == True) & (trix > 0.0), -1, positions) # SELL
        
        return positions
    
###### FUNCTIONS ######
    
def heatmap(data: pd.DataFrame, values: str, index: str, columns: str, title: str, size: tuple) -> go.Heatmap:
    """Produce a 2D Heatmap to inspect parameters combinations.
        
    Args:
        data (pd.DataFrame): data in a form of dataframe.
        values (str): column used for values plotting.
        index (str): column used for y-axis.
        columns (str): column used for x-axis.
        title (str): title of a plot.
        size (tuple): size of a plot (geight, width).    
    """
    # Reformat the dataframe as a pivot table to fit the Heatmap requirements
    data = pd.pivot_table(data, values=values, 
                              index=index,
                              columns=columns)
    # Get rid off scientific notation and as a result show only positive values
    data = np.log10(data)
    # Sort the index
    data = data.sort_index()
    # Create the heatmap plot
    fig = go.Figure(data=go.Heatmap(z=data.values, x=data.columns, y=data.index, hovertemplate='Combination: (%{x}, %{y})<br>Value: %{z}<extra></extra>')) 
    # Update Heatmap parameters
    fig.update_layout(
        xaxis=dict(title=columns),  
        yaxis=dict(title=index),  
        title=title, 
        height=size[0], 
        width=size[1] 
    )
    # Show the Heatmap
    fig.show()
    
    
    
    



