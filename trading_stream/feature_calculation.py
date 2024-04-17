from concurrent.futures import ProcessPoolExecutor
import numpy as np


class FeatureCalculator:
    def __init__(self, features, use_multiprocessing=False):
        self.features = features
        self.n_features = len(self.features)
        self.use_multiprocessing = use_multiprocessing

    def __len__(self):
        return self.n_features

    def calculate_features(self, ticker_data):
        features = np.zeros(len(self))
        if self.use_multiprocessing:
            futures = []
            with ProcessPoolExecutor() as executor:
                for feature in self.features:
                    futures.append(executor.submit(self.features[feature], ticker_data))
            for future in futures:
                features[future.result()['position']] = future.result()['value']
        else:
            for feature in self.features:
                result = self.features[feature](ticker_data)
                features[result['position']] = result['value']
        return features
