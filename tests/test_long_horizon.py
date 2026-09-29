import pandas as pd
import pytest

from datasetsforecast.long_horizon import LongHorizon, LongHorizonInfo


@pytest.mark.parametrize("group,meta", LongHorizonInfo)
def test_longhorizoninfo(group, meta):
    data, *_ = LongHorizon.load(directory='./data', group=group)
    unique_elements = data.groupby(['unique_id', 'ds']).size()
    unique_ts = data.groupby('unique_id').size()

    assert (unique_elements != 1).sum() == 0, f'Duplicated records found: {group}'
    assert unique_ts.shape[0] == meta.n_ts, f'Number of time series not match: {group}'


def test_longhorizon_cache_matches_fresh():
    fresh = LongHorizon.load(directory='./data', group='ETTh1', cache=False)
    LongHorizon.load(directory='./data', group='ETTh1')  # writes the cache
    cached = LongHorizon.load(directory='./data', group='ETTh1')
    for fresh_df, cached_df in zip(fresh, cached):
        if fresh_df is None:
            assert cached_df is None
        else:
            pd.testing.assert_frame_equal(fresh_df, cached_df)
