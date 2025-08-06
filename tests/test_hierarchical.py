import numpy as np
import pytest

from datasetsforecast.hierarchical import HierarchicalData, HierarchicalInfo


@pytest.mark.skip(reason="dropbox download restriction")
@pytest.mark.parametrize("group,meta", HierarchicalInfo)
def test_hierarchicalinfo(group, meta):  # noqa: ARG001
    Y_df, S_df, tags = HierarchicalData.load('./data', group)
    assert all(S_df.loc[cats].values.sum() == S_df.shape[1] for _, cats in tags.items())
    assert len(S_df) == sum(len(v) for _, v in tags.items()), group
    S_hiers = [S_df.loc[cats].values * np.arange(1, len(cats) + 1).reshape(-1, 1) for _, cats in tags.items()]
    S_hiers = np.vstack(S_hiers)
    S_hiers = S_hiers.sum(axis=0)
    is_strictly_hierarchical = np.array_equal(S_hiers, np.sort(S_hiers))
    print(f'Is {group} strictly hierarchical? {is_strictly_hierarchical}')

    # test S recovers Y_df
    for key, hiers in tags.items():
        for ts, bottom_ts in S_df.loc[hiers].iterrows():
            actual_bottom_ts = bottom_ts.loc[lambda x: x == 1].index
            expected_sum = Y_df.query('unique_id == @ts')['y'].sum()
            actual_sum = Y_df.query('unique_id in @actual_bottom_ts')['y'].sum()
            assert np.isclose(expected_sum, actual_sum), f"Sum mismatch for {ts} in {group}: expected {expected_sum}, got {actual_sum}"
