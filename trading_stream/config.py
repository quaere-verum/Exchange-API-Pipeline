# This file should implement the TradingModel class which implements a .trade method taking features as input.
# Include all the relevant import statements, create features and load the relevant models.
import numpy as np
from typing import Callable, Union, List


class TradingModel:
    def __init__(self) -> None:
        self.params = {
            'ticker_history_length': 10,
            'features_history_length': 3,
        }
        self.features = [rolling_mean(window=10,
                                      columns=0)
        ]
        self.n_features = 1

    def trade(self, features: np.ndarray) -> None:
        return


def rolling_mean(window: int, columns: Union[int, List[int], np.ndarray]) -> Callable[[np.ndarray], Union[float, np.ndarray]]:
    def func(ticker_data):
        return np.mean(ticker_data[len(ticker_data)-window:][:, columns])
    return func
