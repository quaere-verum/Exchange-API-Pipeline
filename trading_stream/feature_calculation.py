from concurrent.futures import ProcessPoolExecutor
import numpy as np
from typing import Dict, Callable, Union, Any, List


class FeatureCalculator:
    def __init__(self, features: List[Callable[[np.ndarray], Union[np.ndarray, float]]],
                 n_features: int, use_multiprocessing: bool = False) -> None:
        self.features = features
        self.n_features = n_features
        self.use_multiprocessing = use_multiprocessing

    def __len__(self) -> int:
        return self.n_features

    def calculate_features(self, ticker_data: np.ndarray) -> np.ndarray:
        features = np.zeros(len(self))
        if self.use_multiprocessing:
            futures = []
            with ProcessPoolExecutor() as executor:
                for feature in self.features:
                    futures.append(executor.submit(feature, ticker_data))
            for k, future in enumerate(futures):
                result = future.result()
                if isinstance(result, (float, int)):
                    features[k] = result
                else:
                    features[k:k+len(result)] = result
        else:
            for feature in self.features:
                result = feature(ticker_data)
                features[result['position']] = result['value']
        return features
