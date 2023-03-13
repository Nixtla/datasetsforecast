import dask.dataframe as dd
import fugue.api as fa
import pandas as pd

from .utils import pipeline


def test_dask_actual_y():
    test_df, evaluation_df, evaluation_fn = pipeline()
    test_df = dd.from_pandas(test_df, npartitions=1)
    evaluation_dask = evaluation_fn(test_df).sort_values('metric')
    print(type(evaluation_dask))
    pd.testing.assert_frame_equal(evaluation_df, fa.as_pandas(evaluation_dask))

def test_dask_y_hat_test():
    # Test pass y_hat_df and y_test_df
    test_df, evaluation_df, evaluation_fn = pipeline()
    test_df = dd.from_pandas(test_df, npartitions=1)
    evaluation_dask = evaluation_fn(
        Y_hat_df=fa.drop_columns(test_df, ['y']),
        Y_test_df=fa.select_columns(test_df, ['unique_id', 'ds', 'y'])
    ).sort_values('metric')
    pd.testing.assert_frame_equal(evaluation_df, fa.as_pandas(evaluation_dask))

    
    
