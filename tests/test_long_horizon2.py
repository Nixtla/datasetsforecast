import pytest

from datasetsforecast.long_horizon2 import LongHorizon2, LongHorizon2Info


@pytest.mark.parametrize("group,meta", LongHorizon2Info)
def test_longhorizon2(group, meta):
    Y_df = LongHorizon2.load(directory='data', group=group)
    unique_elements = Y_df.groupby(['unique_id', 'ds']).size()
    unique_ts = Y_df.groupby('unique_id').size()
    n_time = len(Y_df.ds.unique())

    assert (unique_elements != 1).sum() == 0, f'Duplicated records found: {group}'
    assert unique_ts.shape[0] == meta.n_ts, f'Number of time series not match: {group}'
    assert n_time == meta.n_time, f'Number of time observations not match: {group} {n_time} { meta.n_time}'

