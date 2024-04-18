# This file should implement the TradingModel class which implements a .predict method.
# See the feature_calculation class for details on how this is used.
# Include all the relevant import statements, create features and load the relevant models.
import numpy as np
from typing import Callable, Union, List


class TradingModel:
    def __init__(self) -> None:
        self.params = {
            'ticker_shape': None,
            'features_shape': None,
        }
        self.features = [rolling_mean(window=10,
                                      column=0)
        ]

    def predict(self, features: np.ndarray) -> float:
        return 0


def rolling_mean(window: int, column: Union[int, List[int], np.ndarray]) -> Callable[[np.ndarray], Union[float, np.ndarray]]:
    def func(ticker_data):
        return np.mean(ticker_data[len(ticker_data)-window:][:, columns])
    return func
