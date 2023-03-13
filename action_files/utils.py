from functools import partial

import numpy as np
import pandas as pd

from datasetsforecast.evaluation import accuracy
from datasetsforecast.losses import (
    mqloss, scaled_crps, coverage,
    quantile_loss, calibration,
    mae
)


def pipeline():
    y = np.random.normal(size=1_000)
    test_df = pd.DataFrame({
        'unique_id': '1', 
        'ds': np.arange(1_000).astype(int), 
        'model1': np.quantile(y, q=0.5),
        'model2': np.quantile(y, q=0.4),
        'y': y,
    })
    level = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    for lv in level:
        alpha_lo = (100 - lv) / 200
        alpha_hi = 1 - alpha_lo
        for model in ['model1', 'model2']:
            test_df[f'{model}-lo-{lv}'] = np.quantile(test_df['y'], q=alpha_lo)
            test_df[f'{model}-hi-{lv}'] = np.quantile(test_df['y'], q=alpha_hi)
    
    evaluation_fn = partial(
        accuracy,
        metrics=[mqloss, scaled_crps, coverage, quantile_loss, calibration, mae], 
        level=level,
    )
    evaluation_df = evaluation_fn(test_df)
    
    return test_df, evaluation_df, evaluation_fn
