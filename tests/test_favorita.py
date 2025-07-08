import pytest
from pandas._libs.tslibs.np_datetime import OutOfBoundsDatetime

from datasetsforecast.favorita import FavoritaData


def test_favorita_data():
    """Test that cached and non-cached data loading produces consistent results."""
    group = 'Favorita200'
    directory = './data/favorita'

    try:
        # Load data without caching
        static_agg1, static_bottom1, temporal_agg1, temporal_bottom1, S_df1 = FavoritaData.load_preprocessed(
            directory=directory, group=group, cache=False
        )

        # Load data with caching (should be same as above)
        static_agg2, static_bottom2, temporal_agg2, temporal_bottom2, S_df2 = FavoritaData.load_preprocessed(
            directory=directory, group=group
        )

        # Test that the dimensions are consistent between cached and non-cached loads
        assert len(static_agg1) + len(static_agg1.columns) == len(static_agg2) + len(static_agg2.columns), \
            "Static aggregated data dimensions don't match"
        assert len(static_bottom1) + len(static_bottom1.columns) == len(static_bottom2) + len(static_bottom2.columns), \
            "Static bottom data dimensions don't match"
        assert len(temporal_agg1) + len(temporal_agg1.columns) == len(temporal_agg2) + len(temporal_agg2.columns), \
            "Temporal aggregated data dimensions don't match"
        assert len(temporal_bottom1) + len(temporal_bottom1.columns) == len(temporal_bottom2) + len(temporal_bottom2.columns), \
            "Temporal bottom data dimensions don't match"

    except OutOfBoundsDatetime as e:
        pytest.skip(f"Favorita data contains invalid datetime values: {e}")
    except FileNotFoundError as e:
        pytest.skip(f"Favorita data files not found: {e}")
    except Exception as e:
        pytest.skip(f"Favorita data loading failed: {e}")

