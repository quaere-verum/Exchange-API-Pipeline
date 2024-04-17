# This file should implement the TradingModel class which implements a .predict method.
# See the feature_calculation class for details on how this is used.
# Include all the relevant import statements, create features and load the relevant models.
import numpy as np


class TradingModel:
    def __init__(self):
        self.params = {
            'ticker_shape': None,
            'features_shape': None,
        }
        self.features = {
            'rolling_mean_10': rolling_mean(window=10,
                                            column=0,
                                            output_position=0)
        }

    def predict(self, features):
        return 0


def rolling_mean(window, column, output_position):
    def func(ticker_data):
        return {'value': np.mean(ticker_data[len(ticker_data)-window:, column]),
                'position': output_position}
    return func
