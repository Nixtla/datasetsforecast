import pytest

from datasetsforecast.long_horizon import LongHorizon, LongHorizonInfo


@pytest.mark.parametrize("group,meta", LongHorizonInfo)
def test_longhorizoninfo(group, meta):
    data, *_ = LongHorizon.load(directory='./data', group=group)
    unique_elements = data.groupby(['unique_id', 'ds']).size()
    unique_ts = data.groupby('unique_id').size()

    assert (unique_elements != 1).sum() == 0, f'Duplicated records found: {group}'
    assert unique_ts.shape[0] == meta.n_ts, f'Number of time series not match: {group}'
