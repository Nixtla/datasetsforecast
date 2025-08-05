import numpy as np

from datasetsforecast.m4 import M4Evaluation


def test_esrnn_url():
    esrnn_url = 'https://github.com/Nixtla/m4-forecasts/raw/master/forecasts/submission-118.zip'
    esrnn_evaluation = M4Evaluation.evaluate('./data', 'Hourly', esrnn_url)
    # Test of the same evaluation as the original one
    assert np.isclose(esrnn_evaluation['SMAPE'].item(), 9.328, atol=1e-3)
    assert np.isclose(esrnn_evaluation['MASE'].item(), 0.893, atol=1e-3)
    assert np.isclose(esrnn_evaluation['OWA'].item(), 0.440, atol=1e-3)


def test_fforma_url():
    fforma_url = 'https://github.com/Nixtla/m4-forecasts/raw/master/forecasts/submission-245.zip'
    fforma_forecasts = M4Evaluation.load_benchmark('./data', 'Hourly', fforma_url)
    fforma_evaluation = M4Evaluation.evaluate('./data', 'Hourly', fforma_forecasts)
    # Test of the same evaluation as the original one
    assert np.isclose(fforma_evaluation['SMAPE'].item(), 11.506, atol=1e-3)
    assert np.isclose(fforma_evaluation['MASE'].item(), 0.819, atol=1e-3)
    assert np.isclose(fforma_evaluation['OWA'].item(), 0.484, atol=1e-3)

