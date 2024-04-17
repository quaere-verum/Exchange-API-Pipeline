# This file should implement the TradingModel class which implements a .predict method.
# See the feature_calculation class for details on how this is used.
# Include all the relevant import statements, create features and load the relevant models.
# NOTE: every feature should return a dictionary with keys 'value' and 'position'. The features array
# will be an array of shape (n, nr_features), and the concurrent.futures.ProcessPoolExecutor is used to
# calculate features in parallel. Since the result isn't necessarily returned in order,
# the 'position' key should contain the position (0 <= position < nr_features) so the feature calculator knows
# where to insert the value.
import numpy as np
from typing import Callable


class TradingModel:
    def __init__(self) -> None:
        self.params = {
            'ticker_shape': None,
            'features_shape': None,
        }
        self.features = [rolling_mean(window=10,
                                      column=0,
                                      output_position=0)
        ]

    def predict(self, features: np.ndarray) -> float:
        return 0


def rolling_mean(window: int, column: int, output_position: int) -> Callable[[np.ndarray], dict]:
    def func(ticker_data):
        return {'value': np.mean(ticker_data[len(ticker_data)-window:, column]),
                'position': output_position}
    return func
