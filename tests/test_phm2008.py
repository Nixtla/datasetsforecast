import pytest

from datasetsforecast.phm2008 import PHM2008, PHM2008Info


@pytest.mark.parametrize("group,meta", PHM2008Info)
def test_phm2008(group, meta):
    # Checking that load works
    Y_train_df, Y_test_df = PHM2008.load(directory="data", group=group)

    # Checking that train series match info's train series
    unique_elements = Y_train_df.groupby(["unique_id", "ds"]).size()
    unique_ts = Y_train_df.groupby("unique_id").size()
    assert (unique_elements != 1).sum() == 0, f"Duplicated records found: {group}"
    assert unique_ts.shape[0] == meta.n_ts, f"Number of time series not match: {group}"

    # Checking that test series match info's test series
    unique_elements = Y_test_df.groupby(["unique_id", "ds"]).size()
    unique_ts = Y_test_df.groupby("unique_id").size()
    assert (unique_elements != 1).sum() == 0, f"Duplicated records found: {group}"
    assert unique_ts.shape[0] == meta.n_test, (
        f"Number of time series not match: {group}"
    )

    # Assert no nans
    assert not Y_train_df.isnull().values.any()
    assert not Y_test_df.isnull().values.any()
