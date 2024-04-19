# This file should implement the TradingModel class which implements a .trade method taking features as input.
# Include all the relevant import statements, create features and load the relevant models.
import numpy as np
from binance.spot import Spot
from typing import Callable, Union, List, Dict
import time
import os
API_KEY = os.environ['BINANCE_API_KEY']
SECRET_KEY = os.environ['BINANCE_SECRET_KEY']
api_url = 'https://testnet.binance.vision'
client = Spot(api_key=API_KEY, api_secret=SECRET_KEY, **{'base_url': api_url})


class TradingModel:
    def __init__(self) -> None:
        self.params = {
            'ticker_history_length': 10,
            'features_history_length': 3,
        }
        self.features = [rolling_mean(window=10,
                                      columns=[0, 6])
        ]
        self.n_features = 1

    def trade(self, features: np.ndarray, prices: Dict[str, float]) -> None:
        signal = np.random.choice([-1, 0, 1])
        params = None
        match signal:
            case -1:
                params = {
                    'symbol': 'ETHUSDT',
                    'side': 'BUY',
                    'type': 'MARKET',
                    'timeInForce': 'GTC',
                    'quantity': 0.1,
                    'price': 1.,
                    'timestamp': int(time.time()*1e3),
                    'recvWindow': 5000
                }
            case 0:
                return
            case 1:
                params = {
                    'symbol': 'ETHUSDT',
                    'side': 'SELL',
                    'type': 'MARKET',
                    'timeInForce': 'GTC',
                    'quantity': 0.1,
                    'price': 1.,
                    'timestamp': int(time.time() * 1e3),
                    'recvWindow': 5000
                }
        response = client.new_order(**params)
        print(response)
        return


def rolling_mean(window: int, columns: Union[int, List[int], np.ndarray]) -> (
        Callable)[[np.ndarray], Union[float, np.ndarray]]:
    def func(ticker_data):
        return np.mean(ticker_data[len(ticker_data)-window:][:, columns], axis=0)
    return func
