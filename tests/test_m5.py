import numpy as np

from datasetsforecast.m5 import M5, M5Evaluation


def test_m5_winner():
    # Load M5 data first
    M5.load('./data')
    m5_winner_url = 'https://github.com/Nixtla/m5-forecasts/raw/main/forecasts/0001 YJ_STU.zip'
    winner_evaluation = M5Evaluation.evaluate('./data', m5_winner_url)
    # Test of the same evaluation as the original one
    assert np.isclose(winner_evaluation.loc['Total'].item(), 0.520, atol=1e-3)


def test_m5_second_place():
    # Load M5 data first
    M5.load('./data')
    m5_second_place_url = 'https://github.com/Nixtla/m5-forecasts/raw/main/forecasts/0002 Matthias.zip'
    m5_second_place_forecasts = M5Evaluation.load_benchmark('./data', m5_second_place_url)
    second_place_evaluation = M5Evaluation.evaluate('./data', m5_second_place_forecasts)
    # Test of the same evaluation as the original one
    assert np.isclose(second_place_evaluation.loc['Total'].item(), 0.528, atol=1e-3)


def test_m5_benchmark():
    # Load M5 data first
    M5.load('./data')
    winner_benchmark = M5Evaluation.load_benchmark('./data')
    winner_evaluation = M5Evaluation.evaluate('./data', winner_benchmark)
    # Test of the same evaluation as the original one
    assert np.isclose(winner_evaluation.loc['Total'].item(), 0.520, atol=1e-3)
