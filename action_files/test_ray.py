import fugue.api as fa
import ray
import pandas as pd

from .utils import pipeline


def test_ray_actual_y():
    test_df, evaluation_df, evaluation_fn = pipeline()
    test_df = ray.data.from_pandas(test_df).repartition(2)
    evaluation_ray = evaluation_fn(test_df)
    pd.testing.assert_frame_equal(evaluation_df, fa.as_pandas(evaluation_ray))

def test_ray_y_hat_test():
    # Test y_hat_df and y_test_df
    test_df, evaluation_df, evaluation_fn = pipeline()
    test_df = ray.data.from_pandas(test_df).repartition(2)
    evaluation_ray = evaluation_fn(
        Y_hat_df=fa.drop_columns(test_df, ['y']),
        Y_test_df=fa.select_columns(test_df, ['unique_id', 'ds', 'y'])
    )
    pd.testing.assert_frame_equal(evaluation_df, fa.as_pandas(evaluation_ray))

    
    
