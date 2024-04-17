from concurrent.futures import ProcessPoolExecutor
import numpy as np
from typing import Dict, Callable, Union, Any, List


class FeatureCalculator:
    def __init__(self, features: List[Callable[[np.ndarray], Dict[str, Union[np.ndarray, int, float]]]],
                 use_multiprocessing: bool = False) -> None:
        self.features = features
        self.n_features = len(self.features)
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
            for future in futures:
                features[future.result()['position']] = future.result()['value']
        else:
            for feature in self.features:
                result = feature(ticker_data)
                features[result['position']] = result['value']
        return features
